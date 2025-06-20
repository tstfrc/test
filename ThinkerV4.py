import tkinter as tk
from tkinter import filedialog, messagebox
import pandas as pd
from datetime import datetime, timedelta
import os
import json
import base64

def normalize_sharepoint_data(df_spo):
    """Normalizza i dati SharePoint nel formato standard"""
    # Pulisci i nomi delle colonne
    df_spo = clean_column_names(df_spo)
    
    print(f"Colonne disponibili in SharePoint: {list(df_spo.columns)}")
    
    normalized_data = []
    
    for _, row in df_spo.iterrows():
        # Per SharePoint, il "Shared By" è il sito, non un utente reale
        site_name = get_column_value(row, ['Site Name', 'site name', 'Site_Name'], 'Unknown')
        library = get_column_value(row, ['Library', 'library'], 'Unknown')
        
        # Gestisci Users - può essere vuoto per link organization
        users = get_column_value(row, ['Users', 'users'])
        link_type = get_column_value(row, ['Link Type', 'link type', 'Link_Type'], 'Organization Link')
        shared_with = users or link_type
        
        file_folder_name = get_column_value(row, ['File/Folder Name', 'file/folder name', 'File_Folder_Name'], 'Unknown')
        
        normalized_data.append({
            'Shared By': f"SITE: {site_name}",  # Prefisso per identificare i siti
            'Shared With': shared_with,
            'File Name': file_folder_name,
            'Sharing Type': link_type,
            'Shared Resource': get_column_value(row, ['Shared Link', 'shared link', 'Shared_Link']),
            'Shared Time': pd.NaT,  # SharePoint non ha timestamp
            'Source': 'SharePoint',
            'Object Type': get_column_value(row, ['Object Type', 'object type', 'Object_Type'], 'Unknown'),
            'Access Type': get_column_value(row, ['Access Type', 'access type', 'Access_Type'], 'Unknown'),
            'Roles': get_column_value(row, ['Roles', 'roles'], 'Unknown'),
            'Link Status': get_column_value(row, ['Link Status', 'link status', 'Link_Status'], 'Unknown'),
            'Link Expiry Date': get_column_value(row, ['Link Expiry Date', 'link expiry date', 'Link_Expiry_Date']),
            'Password Protected': get_column_value(row, ['Password Protected', 'password protected', 'Password_Protected'], 'False'),
            'Block Download': get_column_value(row, ['Block Download', 'block download', 'Block_Download'], 'False'),
            'Site Name': site_name,
            'Library': library,
            'File Type': get_column_value(row, ['File Type', 'file type', 'File_Type']) or os.path.splitext(str(file_folder_name))[1]
        })
    
    return pd.DataFrame(normalized_data)

def clean_column_names(df):
    """Pulisce i nomi delle colonne rimuovendo spazi extra"""
    df.columns = df.columns.str.strip()
    return df

def get_column_value(row, possible_names, default=''):
    """Cerca una colonna usando nomi possibili"""
    for name in possible_names:
        if name in row and pd.notna(row[name]):
            return row[name]
    return default

def normalize_onedrive_data(df_od):
    """Normalizza i dati OneDrive nel formato standard"""
    # Pulisci i nomi delle colonne
    df_od = clean_column_names(df_od)
    
    print(f"Colonne disponibili in OneDrive: {list(df_od.columns)}")
    
    normalized_data = []
    
    for _, row in df_od.iterrows():
        # Gestisci le date
        shared_time = None
        try:
            created_value = get_column_value(row, ['Created', 'created'])
            if created_value:
                shared_time = pd.to_datetime(created_value, dayfirst=True, errors='coerce')
        except:
            pass
        
        # Cerca le colonne con nomi possibili
        account = get_column_value(row, ['Account', 'account'], 'Unknown')
        access_granted = get_column_value(row, ['Access granted to', 'access granted to', 'Access_granted_to'])
        effective_scope = get_column_value(row, ['Effective scope', 'effective scope', 'Effective_scope'])
        item_name = get_column_value(row, ['ItemName', 'itemname', 'Item Name', 'item name'], 'Unknown')
        
        normalized_data.append({
            'Shared By': account,
            'Shared With': access_granted or effective_scope,
            'File Name': item_name,
            'Sharing Type': effective_scope or 'Unknown',
            'Shared Resource': get_column_value(row, ['ItemUrl', 'itemurl', 'Item URL']),
            'Shared Time': shared_time,
            'Source': 'OneDrive',
            'Folder': get_column_value(row, ['Folder', 'folder']),
            'Permission': get_column_value(row, ['Permission', 'permission'], 'Unknown'),
            'Size': get_column_value(row, ['Size', 'size']),
            'Author': get_column_value(row, ['Author', 'author']),
            'HasPassword': get_column_value(row, ['HasPassword', 'haspassword', 'Has Password'], 'False'),
            'Expiration date': get_column_value(row, ['Expiration date', 'expiration date', 'Expiration_date']),
            'Link Expired': get_column_value(row, ['Link Expired', 'link expired', 'Link_Expired']),
            'Prevents Download': get_column_value(row, ['Prevents Download', 'prevents download', 'Prevents_Download'], 'False'),
            'LastModified': get_column_value(row, ['LastModified', 'lastmodified', 'Last Modified']),
            'Last modified by': get_column_value(row, ['Last modified by', 'last modified by', 'Last_modified_by']),
            'File Type': os.path.splitext(str(item_name))[1] if item_name != 'Unknown' else ''
        })
    
    return pd.DataFrame(normalized_data)

def safe_value_counts(series, max_items=10):
    """Esegue value_counts in modo sicuro, gestendo valori vuoti"""
    try:
        if series is None or len(series) == 0:
            return {}
        # Rimuovi valori NaN e vuoti
        cleaned_series = series.dropna()
        cleaned_series = cleaned_series[cleaned_series != '']
        if len(cleaned_series) == 0:
            return {}
        return cleaned_series.value_counts().head(max_items).to_dict()
    except Exception as e:
        print(f"Errore in safe_value_counts: {e}")
        return {}

def analyze_combined_sharing(df_spo, df_od):
    """Analizza i dati combinati di SharePoint e OneDrive"""
    
    try:
        # Normalizza i dati
        print("Normalizzando dati SharePoint...")
        norm_spo = normalize_sharepoint_data(df_spo)
        print("Normalizzando dati OneDrive...")
        norm_od = normalize_onedrive_data(df_od)
        
        # Combina i dataframe
        df_combined = pd.concat([norm_spo, norm_od], ignore_index=True)
        print(f"Dati combinati: {len(df_combined)} righe totali")
        
        # Salva i dati raw per l'accesso dal frontend
        raw_data = {
            'sharepoint_raw': norm_spo.to_dict('records'),
            'onedrive_raw': norm_od.to_dict('records'),
            'combined_raw': df_combined.to_dict('records')
        }
        
    except Exception as e:
        print(f"Errore durante la normalizzazione: {e}")
        raise e
    
    # Inizializza raw_data all'inizio
    raw_data = {
        'sharepoint_raw': [],
        'onedrive_raw': [],
        'combined_raw': []
    }
    
    # Funzione per calcolare il risk score
    def calculate_risk_score(user_data, source_type='all'):
        score = 0
        external_domains = 0
        
        # Analizza domini esterni
        for _, row in user_data.iterrows():
            shared_with = str(row.get('Shared With', ''))
            if '@' in shared_with:
                emails = shared_with.split(';')
                for email in emails:
                    if '@' in email:
                        domain = email.split('@')[1].strip().lower()
                        if not any(enterprise in domain for enterprise in ['microsoft.com', 'office365.com', 'onmicrosoft.com']):
                            external_domains += 1
                            score += 2
        
        # Punti basati sul volume
        share_count = len(user_data)
        if share_count > 100:
            score += 10
        elif share_count > 50:
            score += 5
        elif share_count > 20:
            score += 2
        
        # Punti per tipi di file rischiosi
        risky_extensions = ['.exe', '.bat', '.ps1', '.vbs', '.doc', '.docx', '.xls', '.xlsx']
        for _, row in user_data.iterrows():
            file_type = str(row.get('File Type', ''))
            if file_type.lower() in risky_extensions:
                score += 1
        
        # Punti per link senza scadenza o password
        for _, row in user_data.iterrows():
            if row.get('Source') == 'SharePoint':
                if str(row.get('Password Protected', '')).lower() == 'false':
                    score += 1
                if 'Never Expires' in str(row.get('Link Expiry Date', '')):
                    score += 2
            elif row.get('Source') == 'OneDrive':
                if str(row.get('HasPassword', '')).lower() == 'false':
                    score += 1
                if pd.isna(row.get('Expiration date')):
                    score += 2
        
        return min(score, 100), external_domains

    # Report principale
    report = {
        'total_shares': len(df_combined),
        'sharepoint_shares': len(norm_spo),
        'onedrive_shares': len(norm_od),
        'unique_sharers': df_combined['Shared By'].nunique(),
        'sharepoint_users': norm_spo['Shared By'].nunique() if len(norm_spo) > 0 else 0,
        'onedrive_users': norm_od['Shared By'].nunique() if len(norm_od) > 0 else 0,
        'shares_by_type': safe_value_counts(df_combined['Sharing Type']),
        'shares_by_source': safe_value_counts(df_combined['Source']),
        'recipient_domains': {},
        'file_types': safe_value_counts(df_combined['File Type'], 15),
        'timeline_data': {},
        'user_activity': {},
        'sharepoint_analysis': {},
        'onedrive_analysis': {},
        'alerts': {},
        'user_details': {},
        'domain_details': {}
    }
    
    # Analisi domini destinatari
    try:
        for _, row in df_combined.iterrows():
            shared_with = str(row.get('Shared With', ''))
            if '@' in shared_with and pd.notna(shared_with):
                emails = shared_with.split(';')
                for email in emails:
                    email = email.strip()
                    if '@' in email and email != '':
                        try:
                            domain = email.split('@')[1].strip()
                            if domain:  # Assicurati che il dominio non sia vuoto
                                report['recipient_domains'][domain] = report['recipient_domains'].get(domain, 0) + 1
                        except IndexError:
                            continue
    except Exception as e:
        print(f"Errore nell'analisi domini: {e}")
    
    # Analisi temporale (solo OneDrive ha timestamp)
    try:
        od_with_date = norm_od.dropna(subset=['Shared Time'])
        if not od_with_date.empty and len(od_with_date) > 0:
            daily_shares = od_with_date.groupby(od_with_date['Shared Time'].dt.date).size()
            report['timeline_data'] = {date.strftime('%Y-%m-%d'): int(count) for date, count in daily_shares.items()}
    except Exception as e:
        print(f"Errore nell'analisi temporale: {e}")
        report['timeline_data'] = {}
    
    # Analisi utenti reali (solo OneDrive) e siti (SharePoint)
    real_users = df_combined[~df_combined['Shared By'].str.startswith('SITE:', na=False)]
    sharepoint_sites = df_combined[df_combined['Shared By'].str.startswith('SITE:', na=False)]
    
    user_shares_counts = safe_value_counts(real_users['Shared By'])
    report['user_activity'] = user_shares_counts
    
    # Analisi siti SharePoint
    site_shares_counts = safe_value_counts(sharepoint_sites['Site Name'])
    report['site_activity'] = site_shares_counts
    
    # Analisi account OneDrive (solo OneDrive users)
    onedrive_users = real_users[real_users['Source'] == 'OneDrive']
    onedrive_accounts = safe_value_counts(onedrive_users['Shared By'])
    report['onedrive_accounts'] = onedrive_accounts
    
    # Analisi specifica SharePoint
    if not norm_spo.empty:
        try:
            report['sharepoint_analysis'] = {
                'by_site': safe_value_counts(norm_spo['Site Name']),
                'by_library': safe_value_counts(norm_spo['Library']),
                'by_object_type': safe_value_counts(norm_spo['Object Type']),
                'by_access_type': safe_value_counts(norm_spo['Access Type']),
                'password_protected': safe_value_counts(norm_spo['Password Protected']),
                'block_download': safe_value_counts(norm_spo['Block Download']),
                'link_status': safe_value_counts(norm_spo['Link Status']),
                'never_expires': len(norm_spo[norm_spo['Link Expiry Date'].astype(str).str.contains('Never Expires', na=False, case=False)])
            }
        except Exception as e:
            print(f"Errore nell'analisi SharePoint: {e}")
            report['sharepoint_analysis'] = {}
    
    # Analisi specifica OneDrive (correggi il calcolo delle scadenze)
    if not norm_od.empty:
        try:
            # Conta correttamente i link con scadenza (non vuoti e non nulli)
            with_expiration = 0
            without_expiration = 0
            
            for _, row in norm_od.iterrows():
                exp_date = row.get('Expiration date')
                if pd.notna(exp_date) and str(exp_date).strip() != '' and str(exp_date).lower() != 'nan':
                    with_expiration += 1
                else:
                    without_expiration += 1
            
            report['onedrive_analysis'] = {
                'by_permission': safe_value_counts(norm_od['Permission']),
                'by_effective_scope': safe_value_counts(norm_od['Sharing Type']),  # Uso Sharing Type invece di Effective scope
                'password_protected': safe_value_counts(norm_od['HasPassword']),
                'prevents_download': safe_value_counts(norm_od['Prevents Download']),
                'with_expiration': with_expiration,
                'without_expiration': without_expiration,
                'total_size': norm_od['Size'].nunique() if 'Size' in norm_od.columns else 0
            }
        except Exception as e:
            print(f"Errore nell'analisi OneDrive: {e}")
            report['onedrive_analysis'] = {}
    
    # Analisi dettagliata utenti reali (solo OneDrive)
    for user in real_users['Shared By'].unique():
        if pd.isna(user):
            continue
            
        user_data = real_users[real_users['Shared By'] == user]
        user_spo = user_data[user_data['Source'] == 'SharePoint']
        user_od = user_data[user_data['Source'] == 'OneDrive']
        
        risk_score, external_domains = calculate_risk_score(user_data)
        
        # Analisi per tipo di condivisione
        links_by_type = {}
        for sharing_type in user_data['Sharing Type'].unique():
            if pd.notna(sharing_type):
                type_data = user_data[user_data['Sharing Type'] == sharing_type]
                links_by_type[sharing_type] = []
                
                for _, row in type_data.iterrows():
                    link_info = {
                        'file_name': row.get('File Name', 'N/A'),
                        'shared_with': row.get('Shared With', 'N/A'),
                        'shared_time': row.get('Shared Time', 'N/A'),
                        'url': row.get('Shared Resource', 'N/A'),
                        'file_type': row.get('File Type', 'N/A'),
                        'source': row.get('Source', 'N/A')
                    }
                    links_by_type[sharing_type].append(link_info)
        
        report['user_details'][user] = {
            'total_shares': len(user_data),
            'sharepoint_shares': len(user_spo),
            'onedrive_shares': len(user_od),
            'risk_score': risk_score,
            'external_domains': external_domains,
            'links_by_type': links_by_type,
            'recent_activity': len(user_data[user_data['Shared Time'] > (datetime.now() - timedelta(days=30))]) if 'Shared Time' in user_data.columns else 0
        }
        
        # Alert per utenti ad alto rischio
        if len(user_data) > 50 or risk_score > 30:
            report['alerts'][user] = {
                'share_count': len(user_data),
                'sharepoint_shares': len(user_spo),
                'onedrive_shares': len(user_od),
                'risk_score': risk_score,
                'external_domains': external_domains,
                'risk_level': 'High' if risk_score > 60 else 'Medium' if risk_score > 30 else 'Low'
            }
    
    # Analisi dettagliata account OneDrive
    report['onedrive_account_details'] = {}
    for account in onedrive_users['Shared By'].unique():
        if pd.isna(account) or account == 'Unknown':
            continue
            
        account_data = onedrive_users[onedrive_users['Shared By'] == account]
        
        # Analisi per permessi
        permissions = {}
        for permission in account_data['Permission'].unique():
            if pd.notna(permission):
                perm_data = account_data[account_data['Permission'] == permission]
                permissions[permission] = {
                    'shares': len(perm_data),
                    'files': perm_data['File Name'].nunique(),
                    'with_expiration': len(perm_data.dropna(subset=['Expiration date']))
                }
        
        # Conta link con e senza scadenza per questo account
        account_with_exp = 0
        account_without_exp = 0
        for _, row in account_data.iterrows():
            exp_date = row.get('Expiration date')
            if pd.notna(exp_date) and str(exp_date).strip() != '' and str(exp_date).lower() != 'nan':
                account_with_exp += 1
            else:
                account_without_exp += 1
        
        report['onedrive_account_details'][account] = {
            'total_shares': len(account_data),
            'permissions': permissions,
            'effective_scopes': safe_value_counts(account_data['Sharing Type']),
            'with_expiration': account_with_exp,
            'without_expiration': account_without_exp,
            'password_protected': len(account_data[account_data['HasPassword'].astype(str).str.lower() == 'true']),
            'prevents_download': len(account_data[account_data['Prevents Download'].astype(str).str.lower() == 'true'])
        }
    report['site_details'] = {}
    for site_name in sharepoint_sites['Site Name'].unique():
        if pd.isna(site_name) or site_name == 'Unknown':
            continue
            
        site_data = sharepoint_sites[sharepoint_sites['Site Name'] == site_name]
        
        # Analisi per libreria
        libraries = {}
        for library in site_data['Library'].unique():
            if pd.notna(library):
                lib_data = site_data[site_data['Library'] == library]
                libraries[library] = {
                    'shares': len(lib_data),
                    'files': lib_data['File Name'].nunique(),
                    'types': safe_value_counts(lib_data['Object Type'])
                }
        
        report['site_details'][site_name] = {
            'total_shares': len(site_data),
            'libraries': libraries,
            'object_types': safe_value_counts(site_data['Object Type']),
            'access_types': safe_value_counts(site_data['Access Type']),
            'never_expires': len(site_data[site_data['Link Expiry Date'].astype(str).str.contains('Never Expires', na=False, case=False)])
        }
    
    # Analisi domini (solo domini reali, non nomi utenti o link organization)
    for _, row in df_combined.iterrows():
        shared_with = str(row.get('Shared With', ''))
        if '@' in shared_with and pd.notna(shared_with):
            emails = shared_with.split(';')
            for email in emails:
                email = email.strip()
                if '@' in email and email != '':
                    # Escludi pattern non-email (Organization link, Anyone in tenant, etc.)
                    if any(x in email.lower() for x in ['organization', 'anyone', 'tenant', 'company', 'internal']):
                        continue
                    
                    try:
                        domain = email.split('@')[1].strip().lower()
                        
                        # Filtri per domini validi:
                        # 1. Deve contenere almeno un punto
                        # 2. Non deve essere troppo lungo (probabilmente è un nome/descrizione)
                        # 3. Non deve contenere spazi
                        # 4. Deve avere un TLD valido (almeno 2 caratteri dopo l'ultimo punto)
                        if (domain and 
                            '.' in domain and 
                            len(domain) < 50 and 
                            ' ' not in domain and
                            len(domain.split('.')[-1]) >= 2 and
                            not any(char in domain for char in ['/', '\\', '?', '#'])):
                            
                            if domain not in report['domain_details']:
                                report['domain_details'][domain] = {
                                    'shares': [],
                                    'sharepoint_shares': 0,
                                    'onedrive_shares': 0,
                                    'risk_level': 'Unknown',
                                    'share_count': 0
                                }
                            
                            link_info = {
                                'file_name': row.get('File Name', 'N/A'),
                                'shared_by': row.get('Shared By', 'N/A'),
                                'shared_with': email,
                                'sharing_type': row.get('Sharing Type', 'N/A'),
                                'shared_time': row.get('Shared Time', 'N/A'),
                                'url': row.get('Shared Resource', 'N/A'),
                                'source': row.get('Source', 'N/A')
                            }
                            report['domain_details'][domain]['shares'].append(link_info)
                            report['domain_details'][domain]['share_count'] += 1
                            
                            if row.get('Source') == 'SharePoint':
                                report['domain_details'][domain]['sharepoint_shares'] += 1
                            else:
                                report['domain_details'][domain]['onedrive_shares'] += 1
                            
                            # Valuta rischio dominio
                            share_count = report['domain_details'][domain]['share_count']
                            if domain.endswith(('.gov', '.edu')):
                                risk_level = 'Low'
                            elif any(enterprise in domain for enterprise in ['microsoft.com', 'google.com', 'amazon.com']):
                                risk_level = 'Low'
                            elif share_count > 20:
                                risk_level = 'High'
                            elif share_count > 5:
                                risk_level = 'Medium'
                            else:
                                risk_level = 'Low'
                            
                            report['domain_details'][domain]['risk_level'] = risk_level
                    except (IndexError, AttributeError):
                        continue
    
    return {
        **report,
        **raw_data  # Aggiungi i dati raw al report
    }

def get_logo_base64(logo_path=None):
    """Codifica il logo in base64"""
    try:
        if logo_path and os.path.exists(logo_path):
            with open(logo_path, 'rb') as image_file:
                encoded_string = base64.b64encode(image_file.read()).decode()
                file_ext = os.path.splitext(logo_path)[1].lower()
                if file_ext in ['.jpg', '.jpeg']:
                    return f"data:image/jpeg;base64,{encoded_string}"
                elif file_ext in ['.png']:
                    return f"data:image/png;base64,{encoded_string}"
                elif file_ext in ['.gif']:
                    return f"data:image/gif;base64,{encoded_string}"
                elif file_ext in ['.svg']:
                    return f"data:image/svg+xml;base64,{encoded_string}"
                else:
                    return f"data:image/png;base64,{encoded_string}"
        else:
            # Logo SVG di default
            return "data:image/svg+xml;base64,PHN2ZyB3aWR0aD0iNjQiIGhlaWdodD0iNjQiIHZpZXdCb3g9IjAgMCA2NCA2NCIgZmlsbD0ibm9uZSIgeG1sbnM9Imh0dHA6Ly93d3cudzMub3JnLzIwMDAvc3ZnIj4KPGNpcmNsZSBjeD0iMzIiIGN5PSIzMiIgcj0iMzAiIHN0cm9rZT0iIzI2NWMzZSIgc3Ryb2tlLXdpZHRoPSIyIiBmaWxsPSIjY2ZmIi8+PHRleHQgeD0iMzIiIHk9IjM3IiBmb250LXNpemU9IjE2IiB0ZXh0LWFuY2hvcj0ibWlkZGxlIiBmaWxsPSIjMmU1YzNlIj5Mb2dvPC90ZXh0Pjwvc3ZnPg=="
    except Exception as e:
        print(f"Errore caricamento logo: {e}")
        return "data:image/svg+xml;base64,PHN2ZyB3aWR0aD0iNjQiIGhlaWdodD0iNjQiIHZpZXdCb3g9IjAgMCA2NCA2NCIgZmlsbD0ibm9uZSIgeG1sbnM9Imh0dHA6Ly93d3cudzMub3JnLzIwMDAvc3ZnIj4KPGNpcmNsZSBjeD0iMzIiIGN5PSIzMiIgcj0iMzAiIHN0cm9rZT0iIzI2NWMzZSIgc3Ryb2tlLXdpZHRoPSIyIiBmaWxsPSIjY2ZmIi8+PHRleHQgeD0iMzIiIHk9IjM3IiBmb250LXNpemU9IjE2IiB0ZXh0LWFuY2hvcj0ibWlkZGxlIiBmaWxsPSIjMmU1YzNlIj5Mb2dvPC90ZXh0Pjwvc3ZnPg=="

def generate_combined_html_report(report, output_path, logo_brainwise_path=None, logo_cliente_path=None):
    """Genera il report HTML combinato con due loghi distinti"""
    logo_brainwise_base64 = get_logo_base64(logo_brainwise_path)
    logo_cliente_base64 = get_logo_base64(logo_cliente_path)

    
    css = '''
    <style>
      * { margin: 0; padding: 0; box-sizing: border-box; }
      body { 
        font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; 
        background: linear-gradient(135deg, #2e5c3e 0%, #4a7c59 50%, #6b9b7a 100%);
        color: #333; 
        min-height: 100vh;
      }
      
      .container { max-width: 1400px; margin: 0 auto; padding: 20px; }
      
      .header { 
        text-align: center; 
        margin-bottom: 30px; 
        background: rgba(255,255,255,0.95);
        padding: 30px;
        border-radius: 15px;
        box-shadow: 0 8px 32px rgba(0,0,0,0.1);
      }
      
      .header-content {
        display: flex;
        align-items: center;
        justify-content: center;
        gap: 20px;
        flex-wrap: wrap;
      }
      
      .logo {
        width: 120px;
        height: 120px;
        object-fit: contain;
        filter: drop-shadow(0 2px 8px rgba(0,0,0,0.1));
      }
      
      .header-text {
        text-align: left;
      }
      
      .header h1 { 
        color: #2e5c3e; 
        font-size: 2.5rem; 
        margin-bottom: 10px;
        background: linear-gradient(45deg, #2e5c3e, #4a7c59);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
      }
      
      .header p { color: #6b9b7a; font-size: 1.1rem; }
      
      .tabs { 
        display: flex; 
        background: rgba(255,255,255,0.9);
        border-radius: 10px;
        padding: 5px;
        margin-bottom: 20px;
        box-shadow: 0 4px 15px rgba(0,0,0,0.1);
        overflow-x: auto;
      }
      
      .tab-button { 
        flex: 1; 
        padding: 15px 20px; 
        border: none; 
        background: transparent;
        cursor: pointer; 
        border-radius: 8px;
        transition: all 0.3s ease;
        font-weight: 600;
        color: #666;
        white-space: nowrap;
        min-width: 120px;
      }
      
      .tab-button.active { 
        background: linear-gradient(45deg, #2e5c3e, #4a7c59);
        color: white;
        transform: translateY(-2px);
        box-shadow: 0 4px 15px rgba(46, 92, 62, 0.3);
      }
      
      .tab-button:hover:not(.active) { 
        background: rgba(46, 92, 62, 0.1);
        transform: translateY(-1px);
      }
      
      .tab-content { 
        display: none; 
        animation: fadeIn 0.5s ease-in;
      }
      
      .tab-content.active { display: block; }
      
      @keyframes fadeIn {
        from { opacity: 0; transform: translateY(20px); }
        to { opacity: 1; transform: translateY(0); }
      }
      
      .card { 
        background: rgba(255,255,255,0.95);
        border-radius: 12px; 
        box-shadow: 0 8px 32px rgba(0,0,0,0.1);
        padding: 25px; 
        margin-bottom: 20px;
        backdrop-filter: blur(10px);
        border: 1px solid rgba(255,255,255,0.2);
      }
      
      .grid { 
        display: grid; 
        grid-template-columns: repeat(auto-fit, minmax(280px, 1fr)); 
        gap: 20px; 
        margin-bottom: 30px;
      }
      
      .grid-2 {
        display: grid;
        grid-template-columns: repeat(auto-fit, minmax(400px, 1fr));
        gap: 20px;
        margin-bottom: 30px;
      }
      
      .stat-card {
        background: linear-gradient(135deg, #2e5c3e 0%, #4a7c59 50%, #6b9b7a 100%);
        color: white;
        text-align: center;
        padding: 30px;
        border-radius: 12px;
        box-shadow: 0 8px 32px rgba(0,0,0,0.2);
        transform: perspective(1000px) rotateX(0deg);
        transition: transform 0.3s ease;
        position: relative;
        overflow: hidden;
      }
      
      .stat-card::before {
        content: '';
        position: absolute;
        top: 0;
        left: -100%;
        width: 100%;
        height: 100%;
        background: linear-gradient(90deg, transparent, rgba(255,255,255,0.2), transparent);
        transition: left 0.7s;
      }
      
      .stat-card:hover {
        transform: perspective(1000px) rotateX(5deg) translateY(-5px);
      }
      
      .stat-card:hover::before {
        left: 100%;
      }
      
      .stat-card h3 { font-size: 1.1rem; margin-bottom: 15px; opacity: 0.9; }
      .stat-card .number { font-size: 3rem; font-weight: bold; margin-bottom: 10px; }
      .stat-card .trend { font-size: 0.9rem; opacity: 0.8; }
      
      .source-badge {
        position: absolute;
        top: 10px;
        right: 10px;
        padding: 4px 8px;
        border-radius: 15px;
        font-size: 0.7rem;
        font-weight: bold;
        text-transform: uppercase;
      }
      
      .source-sharepoint { background: rgba(0, 120, 212, 0.8); }
      .source-onedrive { background: rgba(0, 164, 239, 0.8); }
      
      .btn {
        padding: 10px 20px;
        border: none;
        border-radius: 8px;
        font-weight: 600;
        cursor: pointer;
        transition: all 0.3s ease;
        font-size: 14px;
      }
      
      .btn-primary {
        background: linear-gradient(45deg, #2e5c3e, #4a7c59);
        color: white;
      }
      
      .btn-primary:hover {
        transform: translateY(-2px);
        box-shadow: 0 4px 15px rgba(46, 92, 62, 0.4);
      }
      
      table { 
        width: 100%; 
        border-collapse: collapse; 
        margin-top: 15px;
        background: white;
        border-radius: 8px;
        overflow: hidden;
        box-shadow: 0 4px 15px rgba(0,0,0,0.1);
      }
      
      th, td { 
        padding: 12px 15px; 
        text-align: left; 
        border-bottom: 1px solid #eee;
        word-wrap: break-word;
        max-width: 200px;
      }
      
      th { 
        background: linear-gradient(45deg, #2e5c3e, #4a7c59);
        color: white; 
        font-weight: 600;
        cursor: pointer; 
        position: relative;
        transition: all 0.3s ease;
        user-select: none;
      }
      
      th:hover { 
        background: linear-gradient(45deg, #1e4c2e, #3a6c49); 
        transform: translateY(-1px);
      }
      
      th:active {
        transform: translateY(0);
      }
      
      .chart-container canvas {
        cursor: pointer;
        transition: transform 0.2s ease;
      }
      
      .chart-container canvas:hover {
        transform: scale(1.02);
      }
      
      .clickable-chart {
        position: relative;
      }
      
      .clickable-chart::after {
        content: "👆 Clicca per dettagli";
        position: absolute;
        top: 10px;
        right: 10px;
        background: rgba(46, 92, 62, 0.9);
        color: white;
        padding: 5px 10px;
        border-radius: 15px;
        font-size: 0.8rem;
        opacity: 0;
        transition: opacity 0.3s ease;
        pointer-events: none;
      }
      
      .clickable-chart:hover::after {
        opacity: 1;
      }
      
      .meta-label {
        font-weight: 600;
        color: #2e5c3e;
        font-size: 0.8rem;
        text-transform: uppercase;
        letter-spacing: 0.5px;
        margin-bottom: 4px;
        display: block;
      }
      
      .meta-value {
        color: #333;
        font-weight: 500;
        word-break: break-word;
        line-height: 1.4;
      }
      
      .meta-link {
        color: #2e5c3e;
        text-decoration: none;
        font-weight: 500;
        word-break: break-all;
        display: block;
        padding: 4px 0;
      }
      
      .meta-link:hover {
        color: #4a7c59;
        text-decoration: underline;
      }
      
      tr:nth-child(even) { background: #f8f9fa; }
      tr:hover { background: #e8f5e8; transform: scale(1.005); transition: all 0.2s ease; }
      
      .link { 
        color: #2e5c3e; 
        text-decoration: none; 
        cursor: pointer;
        font-weight: 500;
        transition: color 0.3s ease;
        word-break: break-all;
      }
      
      .link:hover { 
        color: #4a7c59; 
        text-decoration: underline;
      }
      
      .search-box {
        width: 100%;
        padding: 12px 20px;
        border: 2px solid #ddd;
        border-radius: 25px;
        font-size: 16px;
        margin-bottom: 20px;
        transition: border-color 0.3s ease;
      }
      
      .search-box:focus {
        outline: none;
        border-color: #2e5c3e;
        box-shadow: 0 0 10px rgba(46, 92, 62, 0.3);
      }
      
      .badge { 
        color: white; 
        padding: 4px 12px; 
        border-radius: 20px; 
        font-size: 12px;
        font-weight: 600;
        display: inline-block;
        margin: 2px;
      }
      
      .badge-high { background: linear-gradient(45deg, #d63031, #e17055); }
      .badge-medium { background: linear-gradient(45deg, #fdcb6e, #e17055); }
      .badge-low { background: linear-gradient(45deg, #00b894, #00a085); }
      .badge-info { background: linear-gradient(45deg, #74b9ff, #0984e3); }
      .badge-sharepoint { background: linear-gradient(45deg, #0078d4, #106ebe); }
      .badge-onedrive { background: linear-gradient(45deg, #0078d4, #004578); }
      
      .chart-container {
        background: white;
        padding: 20px;
        border-radius: 12px;
        margin-bottom: 20px;
        box-shadow: 0 4px 15px rgba(0,0,0,0.1);
      }
      
      .chart-grid {
        display: grid;
        grid-template-columns: repeat(auto-fit, minmax(500px, 1fr));
        gap: 20px;
        margin-bottom: 30px;
      }
      
      .footer-disclaimer {
        background: rgba(255,255,255,0.95);
        padding: 20px;
        border-radius: 12px;
        margin-top: 30px;
        text-align: center;
        font-size: 0.9rem;
        color: #666;
        border-left: 4px solid #2e5c3e;
      }
      
      /* Modal styles */
      .modal {
        display: none;
        position: fixed;
        z-index: 1000;
        left: 0;
        top: 0;
        width: 100%;
        height: 100%;
        background: rgba(0,0,0,0.8);
        backdrop-filter: blur(5px);
      }
      
      .modal-content {
        background: white;
        margin: 2% auto;
        padding: 0;
        border-radius: 15px;
        width: 95%;
        max-width: 1200px;
        max-height: 90vh;
        overflow: hidden;
        box-shadow: 0 20px 60px rgba(0,0,0,0.3);
        animation: modalSlideIn 0.3s ease;
        display: flex;
        flex-direction: column;
      }
      
      .modal-header {
        padding: 20px 30px;
        border-bottom: 1px solid #eee;
        background: linear-gradient(45deg, #2e5c3e, #4a7c59);
        color: white;
        display: flex;
        justify-content: space-between;
        align-items: center;
        flex-shrink: 0;
      }
      
      .modal-body {
        flex: 1;
        overflow-y: auto;
        padding: 20px 30px;
        max-height: calc(90vh - 160px);
        position: relative;
      }
      
      .close {
        color: white;
        font-size: 28px;
        font-weight: bold;
        cursor: pointer;
        transition: color 0.3s ease;
        opacity: 0.8;
      }
      
      .close:hover { opacity: 1; }
      
      @keyframes modalSlideIn {
        from { transform: translateY(-50px); opacity: 0; }
        to { transform: translateY(0); opacity: 1; }
      }
      /* Legenda Risk Score */
.legend-card {
    margin-bottom: 20px;
    background: linear-gradient(135deg, rgba(255,255,255,0.98) 0%, rgba(248,250,252,0.95) 100%);
    border-left: 4px solid #3498db;
}

.legend-card h3 {
    color: #2e5c3e;
    margin-bottom: 10px;
    font-size: 1.2rem;
}

.legend-description {
    color: #666;
    margin-bottom: 15px;
    font-style: italic;
}

.legend-grid {
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(250px, 1fr));
    gap: 12px;
}

.legend-item {
    display: flex;
    align-items: center;
    gap: 10px;
    padding: 8px 12px;
    border-radius: 8px;
    background: rgba(255,255,255,0.7);
    border: 1px solid rgba(0,0,0,0.1);
    transition: all 0.3s ease;
}

.legend-item:hover {
    transform: translateY(-2px);
    box-shadow: 0 4px 12px rgba(0,0,0,0.1);
}

.risk-indicator {
    font-size: 1.2em;
    flex-shrink: 0;
}

.risk-info {
    display: flex;
    flex-direction: column;
    gap: 2px;
}

.risk-info strong {
    color: #2c3e50;
    font-size: 0.9rem;
}

.risk-info span {
    color: #666;
    font-size: 0.8rem;
    line-height: 1.3;
}

/* Colori specifici per ogni livello di rischio */
.risk-high {
    border-left: 3px solid #e74c3c;
}

.risk-medium-high {
    border-left: 3px solid #f39c12;
}

.risk-medium {
    border-left: 3px solid #ff9500;
}

.risk-low {
    border-left: 3px solid #27ae60;
}

/* Responsive per mobile */
@media (max-width: 768px) {
    .legend-grid {
        grid-template-columns: 1fr;
    }
    
    .legend-item {
        padding: 10px;
    }
    
    .risk-info strong {
        font-size: 0.85rem;
    }
    
    .risk-info span {
        font-size: 0.75rem;
    }
}
      
      /* Responsive Design */
      @media (max-width: 768px) {
        .container { padding: 10px; }
        .header h1 { font-size: 2rem; }
        .header-content { flex-direction: column; text-align: center; }
        .header-text { text-align: center; }
        .tabs { flex-direction: column; }
        .tab-button { margin-bottom: 5px; }
        .modal-content { margin: 5% auto; width: 98%; }
        .logo { width: 48px; height: 48px; }
        .grid { grid-template-columns: 1fr; }
        .chart-grid { grid-template-columns: 1fr; }
      }

      .grid-4 {
    display: grid;
    grid-template-columns: auto 1fr 1fr 1fr; /* Logo + 3 tiles */
    gap: 20px;
    align-items: center;
    margin-bottom: 30px;
}

.placeholder-card {
    display: flex;
    align-items: center;
    justify-content: center;
    background: rgba(255,255,255,0.95);
    padding: 20px;
    border-radius: 12px;
    box-shadow: 0 4px 15px rgba(0,0,0,0.1);
}

/* Responsive */
@media (max-width: 768px) {
    .grid-4 {
        grid-template-columns: 1fr;
        gap: 15px;
    }
    
    .placeholder-card {
        order: -1;
    }
}
    </style>'''

    js = '''
    <script src="https://cdnjs.cloudflare.com/ajax/libs/Chart.js/3.9.1/chart.min.js"></script>
    <script>
    let reportData = ''' + json.dumps(report, default=str) + ''';
    
    // Tab Management (updated to handle programmatic calls)
    function showTab(tabName, clickEvent = null) {
        document.querySelectorAll('.tab-content').forEach(tab => {
            tab.classList.remove('active');
        });
        
        document.querySelectorAll('.tab-button').forEach(btn => {
            btn.classList.remove('active');
        });
        
        const tabElement = document.getElementById(tabName);
        if (tabElement) {
            tabElement.classList.add('active');
            
            // Find and activate the corresponding tab button
            const tabButtons = document.querySelectorAll('.tab-button');
            tabButtons.forEach(btn => {
                if ((tabName === 'summary' && btn.textContent.includes('Panoramica')) ||
                    (tabName === 'sharepoint' && btn.textContent.includes('SharePoint')) ||
                    (tabName === 'onedrive' && btn.textContent.includes('OneDrive')) ||
                    (tabName === 'users' && btn.textContent.includes('Utenti')) ||
                    (tabName === 'domains' && btn.textContent.includes('Domini'))) {
                    btn.classList.add('active');
                }
            });
            
            // If called from an event (button click), also mark that button as active
            if (clickEvent && clickEvent.target) {
                clickEvent.target.classList.add('active');
            }
        }
        
        // Inizializza grafici specifici per tab
        setTimeout(() => {
            if (tabName === 'summary') {
                initSummaryCharts();
            } else if (tabName === 'sharepoint') {
                initSharePointCharts();
            } else if (tabName === 'onedrive') {
                initOneDriveCharts();
            }
        }, 100);
    }
    
    // Grafici Summary
    function initSummaryCharts() {
        // Grafico distribuzione per sorgente
        const sourceCtx = document.getElementById('sourceChart');
        if (sourceCtx && reportData.shares_by_source) {
            new Chart(sourceCtx, {
                type: 'doughnut',
                data: {
                    labels: Object.keys(reportData.shares_by_source),
                    datasets: [{
                        data: Object.values(reportData.shares_by_source),
                        backgroundColor: ['#0078d4', '#106ebe']
                    }]
                },
                options: {
                    responsive: true,
                    plugins: {
                        legend: { position: 'bottom' },
                        title: { display: true, text: 'Condivisioni per Sorgente' }
                    }
                }
            });
        }
        
        // Grafico timeline
        const timelineCtx = document.getElementById('timelineChart');
        if (timelineCtx && reportData.timeline_data) {
            const dates = Object.keys(reportData.timeline_data).sort();
            const values = dates.map(date => reportData.timeline_data[date]);
            
            new Chart(timelineCtx, {
                type: 'line',
                data: {
                    labels: dates.map(date => new Date(date).toLocaleDateString()),
                    datasets: [{
                        label: 'Condivisioni Giornaliere',
                        data: values,
                        borderColor: '#2e5c3e',
                        backgroundColor: 'rgba(46, 92, 62, 0.1)',
                        fill: true,
                        tension: 0.4
                    }]
                },
                options: {
                    responsive: true,
                    plugins: {
                        title: { display: true, text: 'Attività di Condivisione nel Tempo' }
                    },
                    scales: {
                        y: { beginAtZero: true }
                    }
                }
            });
        }
    }

    //Sorttable columns
    function sortTable(table, columnIndex, dataType) {
    let rows = Array.from(table.rows).slice(1); // Escludi l'header
    let isAscending = table.getAttribute('data-sort-direction') !== 'asc';
    
    rows.sort((a, b) => {
        let aValue = a.cells[columnIndex].textContent.trim();
        let bValue = b.cells[columnIndex].textContent.trim();
        
        if (dataType === 'number') {
            // Rimuovi caratteri non numerici (virgole, spazi)
            aValue = parseFloat(aValue.replace(/[^\d.-]/g, '')) || 0;
            bValue = parseFloat(bValue.replace(/[^\d.-]/g, '')) || 0;
            return isAscending ? aValue - bValue : bValue - aValue;
        } else {
            // Ordinamento stringa
            return isAscending ? 
                aValue.localeCompare(bValue) : 
                bValue.localeCompare(aValue);
        }
    });
    
    // Rimuovi le righe esistenti
    while (table.rows.length > 1) {
        table.deleteRow(1);
    }
    
    // Aggiungi le righe ordinate
    rows.forEach(row => table.appendChild(row));
    
    // Aggiorna la direzione di ordinamento
    table.setAttribute('data-sort-direction', isAscending ? 'asc' : 'desc');
    
    // Aggiorna gli indicatori visivi
    updateSortIndicators(table, columnIndex, isAscending);
}

function updateSortIndicators(table, activeColumn, isAscending) {
    // Rimuovi tutti gli indicatori esistenti
    Array.from(table.querySelectorAll('th')).forEach((th, index) => {
        th.classList.remove('sort-asc', 'sort-desc');
        if (index === activeColumn) {
            th.classList.add(isAscending ? 'sort-asc' : 'sort-desc');
        }
    });
}
    
    // Grafici SharePoint specifici (rimosso grafico sicurezza)
    function initSharePointCharts() {
        // Grafico per siti (con click handler)
        const siteCtx = document.getElementById('siteChart');
        if (siteCtx && reportData.sharepoint_analysis.by_site) {
            const chart = new Chart(siteCtx, {
                type: 'bar',
                data: {
                    labels: Object.keys(reportData.sharepoint_analysis.by_site),
                    datasets: [{
                        label: 'Condivisioni per Sito',
                        data: Object.values(reportData.sharepoint_analysis.by_site),
                        backgroundColor: '#0078d4',
                        borderColor: '#106ebe',
                        borderWidth: 1
                    }]
                },
                options: {
                    responsive: true,
                    plugins: {
                        title: { display: true, text: 'Condivisioni per Sito SharePoint (Clicca per dettagli)' }
                    },
                    onClick: (event, elements) => {
                        if (elements.length > 0) {
                            const index = elements[0].index;
                            const siteName = Object.keys(reportData.sharepoint_analysis.by_site)[index];
                            showSiteDetails(siteName);
                        }
                    },
                    scales: {
                        y: {
                            beginAtZero: true,
                            title: { display: true, text: 'Numero Condivisioni' }
                        }
                    }
                }
            });
        }
    }
    
    // Grafici OneDrive specifici
    function initOneDriveCharts() {
        // Grafico permessi
        const permCtx = document.getElementById('permissionChart');
        if (permCtx && reportData.onedrive_analysis.by_permission) {
            new Chart(permCtx, {
                type: 'pie',
                data: {
                    labels: Object.keys(reportData.onedrive_analysis.by_permission),
                    datasets: [{
                        data: Object.values(reportData.onedrive_analysis.by_permission),
                        backgroundColor: ['#0078d4', '#106ebe', '#004578', '#74b9ff']
                    }]
                },
                options: {
                    responsive: true,
                    plugins: {
                        title: { display: true, text: 'Distribuzione Permessi OneDrive' }
                    }
                }
            });
        }
        
        // Grafico sicurezza OneDrive
        const securityCtx = document.getElementById('securityChart');
        if (securityCtx) {
            const withExpiration = reportData.onedrive_analysis.with_expiration || 0;
            const withoutExpiration = reportData.onedrive_analysis.without_expiration || 0;
            const passwordProtected = reportData.onedrive_analysis.password_protected?.['True'] || 0;
            const passwordUnprotected = reportData.onedrive_analysis.password_protected?.['False'] || 0;
            
            new Chart(securityCtx, {
                type: 'bar',
                data: {
                    labels: ['Con Scadenza', 'Senza Scadenza', 'Con Password', 'Senza Password'],
                    datasets: [{
                        label: 'Numero di Link',
                        data: [withExpiration, withoutExpiration, passwordProtected, passwordUnprotected],
                        backgroundColor: ['#00b894', '#d63031', '#0984e3', '#fdcb6e'],
                        borderColor: ['#00a085', '#b71c1c', '#0056b3', '#e17055'],
                        borderWidth: 1
                    }]
                },
                options: {
                    responsive: true,
                    plugins: {
                        title: { display: true, text: 'Analisi Sicurezza OneDrive' }
                    },
                    scales: {
                        y: {
                            beginAtZero: true,
                            title: { display: true, text: 'Numero di Link' }
                        }
                    }
                }
            });
        }
    }
    
    // Tabelle utenti (solo utenti reali, non siti)
    function updateUserTable() {
        const tbody = document.querySelector('#usersTable tbody');
        if (!tbody) return;
        
        tbody.innerHTML = '';
        
        Object.entries(reportData.user_details).forEach(([user, data]) => {
            // Filtra i siti (che iniziano con "SITE:")
            if (user.startsWith('SITE:')) return;
            
            const riskLevel = data.risk_score > 60 ? 'High' : data.risk_score > 30 ? 'Medium' : 'Low';
            const riskClass = riskLevel.toLowerCase();
            
            const row = `
                <tr>
                    <td><a href="javascript:void(0)" class="link" onclick="showUserDetails('${user}')">${user}</a></td>
                    <td>${data.total_shares}</td>
                    <td><span class="badge badge-sharepoint">${data.sharepoint_shares}</span></td>
                    <td><span class="badge badge-onedrive">${data.onedrive_shares}</span></td>
                    <td><span class="badge badge-${riskClass}">${riskLevel} (${data.risk_score})</span></td>
                    <td>${data.external_domains}</td>
                    <td><button class="btn btn-primary" onclick="showUserDetails('${user}')">Dettagli</button></td>
                </tr>`;
            tbody.innerHTML += row;
        });
    }
    
    // Tabelle account OneDrive
    function updateOneDriveTable() {
        const tbody = document.querySelector('#onedriveTable tbody');
        if (!tbody) return;
        
        tbody.innerHTML = '';
        
        Object.entries(reportData.onedrive_account_details || {}).forEach(([account, data]) => {
            const row = `
                <tr>
                    <td><a href="javascript:void(0)" class="link" onclick="showOneDriveDetails('${account}')">${account}</a></td>
                    <td>${data.total_shares}</td>
                    <td><span class="badge badge-info">${data.with_expiration}</span></td>
                    <td><span class="badge badge-medium">${data.without_expiration}</span></td>
                    <td><span class="badge badge-low">${data.password_protected}</span></td>
                    <td><button class="btn btn-primary" onclick="showOneDriveDetails('${account}')">Dettagli</button></td>
                </tr>`;
            tbody.innerHTML += row;
        });
    }
    function updateSiteTable() {
        const tbody = document.querySelector('#sitesTable tbody');
        if (!tbody) return;
        
        tbody.innerHTML = '';
        
        Object.entries(reportData.site_details || {}).forEach(([site, data]) => {
            const row = `
                <tr>
                    <td><a href="javascript:void(0)" class="link" onclick="showSiteDetails('${site}')">${site}</a></td>
                    <td>${data.total_shares}</td>
                    <td>${Object.keys(data.libraries || {}).length}</td>
                    <td>${data.never_expires || 0}</td>
                    <td><button class="btn btn-primary" onclick="showSiteDetails('${site}')">Dettagli</button></td>
                </tr>`;
            tbody.innerHTML += row;
        });
    }
    
    // Tabelle domini
    function updateDomainTable() {
        const tbody = document.querySelector('#domainsTable tbody');
        if (!tbody) return;
        
        tbody.innerHTML = '';
        
        Object.entries(reportData.domain_details || {}).forEach(([domain, data]) => {
            const riskClass = data.risk_level.toLowerCase();
            
            const row = `
                <tr>
                    <td title="${domain}">${domain}</td>
                    <td>${data.share_count}</td>
                    <td><span class="badge badge-sharepoint">${data.sharepoint_shares}</span></td>
                    <td><span class="badge badge-onedrive">${data.onedrive_shares}</span></td>
                    <td><span class="badge badge-${riskClass}">${data.risk_level}</span></td>
                    <td><button class="btn btn-primary" onclick="showDomainDetails('${domain}')">Dettagli</button></td>
                </tr>`;
            tbody.innerHTML += row;
        });
    }
    
    // Modal per dettagli sito (usando la stessa logica degli utenti)
    function showSiteDetails(siteName) {
        const modal = document.getElementById('siteModal');
        const content = document.getElementById('siteModalContent');
        
        // Trova l'utente "sito" corrispondente nei user_details
        let siteUserKey = null;
        let siteData = null;
        
        Object.keys(reportData.user_details || {}).forEach(userKey => {
            if (userKey.startsWith('SITE:') && userKey.includes(siteName)) {
                siteUserKey = userKey;
                siteData = reportData.user_details[userKey];
            }
        });
        
        if (siteData) {
            let html = `
                <div class="modal-header">
                    <h2>🏢 ${siteName}</h2>
                    <span class="close" onclick="closeModal('siteModal')">&times;</span>
                </div>
                <div class="modal-body">
                    <div class="grid">
                        <div class="stat-card">
                            <h3>Totale Condivisioni</h3>
                            <div class="number">${siteData.total_shares}</div>
                        </div>
                        <div class="stat-card">
                            <h3>SharePoint</h3>
                            <div class="number">${siteData.sharepoint_shares}</div>
                            <div class="source-badge source-sharepoint">SharePoint</div>
                        </div>
                        <div class="stat-card">
                            <h3>OneDrive</h3>
                            <div class="number">${siteData.onedrive_shares}</div>
                            <div class="source-badge source-onedrive">OneDrive</div>
                        </div>
                        <div class="stat-card">
                            <h3>Risk Score</h3>
                            <div class="number">${siteData.risk_score}</div>
                            <div class="trend">Livello di rischio</div>
                        </div>
                    </div>
                    
                    <h3>Dettagli Condivisioni</h3>
                    <div style="max-height: 400px; overflow-y: auto;">`;
            
            for (const [type, links] of Object.entries(siteData.links_by_type || {})) {
                if (links.length > 0) {
                    html += `<h4 style="margin: 20px 0 15px 0; color: #2e5c3e;">📁 ${type} (${links.length} condivisioni)</h4>`;
                    
                    links.forEach(link => {
                        const sharedTime = link.shared_time && link.shared_time !== 'N/A' 
                            ? new Date(link.shared_time).toLocaleString() 
                            : 'N/A';
                        
                        html += `
                            <div class="sharing-card" style="background: #f8f9fa; border-radius: 12px; padding: 20px; margin-bottom: 15px; border-left: 4px solid #2e5c3e;">
                                <div class="sharing-card-header" style="display: flex; justify-content: space-between; align-items: flex-start; margin-bottom: 15px;">
                                    <div class="sharing-card-title" style="font-weight: 600; color: #2e5c3e; font-size: 1.1rem; word-break: break-word; flex: 1;">${link.file_name}</div>
                                    <span class="badge badge-sharepoint">${type}</span>
                                </div>
                                <div class="sharing-card-meta" style="display: grid; grid-template-columns: 1fr; gap: 12px; font-size: 0.9rem; color: #666;">
                                    <div class="meta-item" style="display: flex; flex-direction: column; gap: 4px; padding: 8px 0; border-bottom: 1px solid #eee;">
                                        <div class="meta-label">👤 Condiviso con</div>
                                        <div class="meta-value">${link.shared_with}</div>
                                    </div>
                                    <div class="meta-item" style="display: flex; flex-direction: column; gap: 4px; padding: 8px 0; border-bottom: 1px solid #eee;">
                                        <div class="meta-label">📅 Data</div>
                                        <div class="meta-value">${sharedTime}</div>
                                    </div>
                                    <div class="meta-item" style="display: flex; flex-direction: column; gap: 4px; padding: 8px 0; border-bottom: 1px solid #eee;">
                                        <div class="meta-label">📎 Tipo File</div>
                                        <div class="meta-value">${link.file_type}</div>
                                    </div>`;
                        
                        if (link.url !== 'N/A') {
                            html += `
                                    <div class="meta-item" style="display: flex; flex-direction: column; gap: 4px; padding: 8px 0;">
                                        <div class="meta-label">🔗 Link</div>
                                        <a href="${link.url}" target="_blank" class="meta-link">${link.url}</a>
                                    </div>`;
                        }
                        
                        html += `
                                </div>
                            </div>`;
                    });
                }
            }
            
            html += `
                        </div>
                    </div>
                </div>`;
            
            content.innerHTML = html;
            modal.style.display = 'block';
        }
    }
    
    // Modal per dettagli account OneDrive (usando la stessa logica degli utenti)
    function showOneDriveDetails(accountName) {
        const modal = document.getElementById('onedriveModal');
        const content = document.getElementById('onedriveModalContent');
        
        if (reportData.user_details[accountName]) {
            const userData = reportData.user_details[accountName];
            
            let html = `
                <div class="modal-header">
                    <h2>☁️ ${accountName}</h2>
                    <span class="close" onclick="closeModal('onedriveModal')">&times;</span>
                </div>
                <div class="modal-body">
                    <div class="grid">
                        <div class="stat-card">
                            <h3>Totale Condivisioni</h3>
                            <div class="number">${userData.total_shares}</div>
                        </div>
                        <div class="stat-card">
                            <h3>SharePoint</h3>
                            <div class="number">${userData.sharepoint_shares}</div>
                            <div class="source-badge source-sharepoint">SharePoint</div>
                        </div>
                        <div class="stat-card">
                            <h3>OneDrive</h3>
                            <div class="number">${userData.onedrive_shares}</div>
                            <div class="source-badge source-onedrive">OneDrive</div>
                        </div>
                        <div class="stat-card">
                            <h3>Risk Score</h3>
                            <div class="number">${userData.risk_score}</div>
                            <div class="trend">Livello: ${userData.risk_score > 60 ? 'High' : userData.risk_score > 30 ? 'Medium' : 'Low'}</div>
                        </div>
                    </div>
                    
                    <h3>Dettagli Condivisioni</h3>
                    <div style="max-height: 400px; overflow-y: auto;">`;
            
            for (const [type, links] of Object.entries(userData.links_by_type || {})) {
                if (links.length > 0) {
                    html += `<h4 style="margin: 20px 0 15px 0; color: #2e5c3e;">📁 ${type} (${links.length} condivisioni)</h4>`;
                    
                    links.forEach(link => {
                        const sharedTime = link.shared_time && link.shared_time !== 'N/A' 
                            ? new Date(link.shared_time).toLocaleString() 
                            : 'N/A';
                        
                        html += `
                            <div class="sharing-card" style="background: #f8f9fa; border-radius: 12px; padding: 20px; margin-bottom: 15px; border-left: 4px solid #2e5c3e;">
                                <div class="sharing-card-header" style="display: flex; justify-content: space-between; align-items: flex-start; margin-bottom: 15px;">
                                    <div class="sharing-card-title" style="font-weight: 600; color: #2e5c3e; font-size: 1.1rem; word-break: break-word; flex: 1;">${link.file_name}</div>
                                    <span class="badge badge-${link.source.toLowerCase()}">${type}</span>
                                </div>
                                <div class="sharing-card-meta" style="display: grid; grid-template-columns: 1fr; gap: 12px; font-size: 0.9rem; color: #666;">
                                    <div class="meta-item" style="display: flex; flex-direction: column; gap: 4px; padding: 8px 0; border-bottom: 1px solid #eee;">
                                        <div class="meta-label">👤 Condiviso con</div>
                                        <div class="meta-value">${link.shared_with}</div>
                                    </div>
                                    <div class="meta-item" style="display: flex; flex-direction: column; gap: 4px; padding: 8px 0; border-bottom: 1px solid #eee;">
                                        <div class="meta-label">📅 Data</div>
                                        <div class="meta-value">${sharedTime}</div>
                                    </div>
                                    <div class="meta-item" style="display: flex; flex-direction: column; gap: 4px; padding: 8px 0; border-bottom: 1px solid #eee;">
                                        <div class="meta-label">📎 Tipo File</div>
                                        <div class="meta-value">${link.file_type}</div>
                                    </div>
                                    <div class="meta-item" style="display: flex; flex-direction: column; gap: 4px; padding: 8px 0; border-bottom: 1px solid #eee;">
                                        <div class="meta-label">🔗 Sorgente</div>
                                        <div class="meta-value">${link.source}</div>
                                    </div>`;
                        
                        if (link.url !== 'N/A') {
                            html += `
                                    <div class="meta-item" style="display: flex; flex-direction: column; gap: 4px; padding: 8px 0;">
                                        <div class="meta-label">🔗 Link</div>
                                        <a href="${link.url}" target="_blank" class="meta-link">${link.url}</a>
                                    </div>`;
                        }
                        
                        html += `
                                </div>
                            </div>`;
                    });
                }
            }
            
            html += `
                        </div>
                    </div>
                </div>`;
            
            content.innerHTML = html;
            modal.style.display = 'block';
        }
    }
    function showUserDetails(username) {
        const modal = document.getElementById('userModal');
        const content = document.getElementById('userModalContent');
        
        if (reportData.user_details[username]) {
            const userData = reportData.user_details[username];
            const riskLevel = userData.risk_score > 60 ? 'High' : userData.risk_score > 30 ? 'Medium' : 'Low';
            
            let html = `
                <div class="modal-header">
                    <h2>👤 ${username}</h2>
                    <span class="close" onclick="closeModal('userModal')">&times;</span>
                </div>
                <div class="modal-body">
                    <div class="grid">
                        <div class="stat-card">
                            <h3>Totale Condivisioni</h3>
                            <div class="number">${userData.total_shares}</div>
                        </div>
                        <div class="stat-card">
                            <h3>SharePoint</h3>
                            <div class="number">${userData.sharepoint_shares}</div>
                            <div class="source-badge source-sharepoint">SharePoint</div>
                        </div>
                        <div class="stat-card">
                            <h3>OneDrive</h3>
                            <div class="number">${userData.onedrive_shares}</div>
                            <div class="source-badge source-onedrive">OneDrive</div>
                        </div>
                        <div class="stat-card">
                            <h3>Risk Score</h3>
                            <div class="number">${userData.risk_score}</div>
                            <div class="trend">Livello: ${riskLevel}</div>
                        </div>
                    </div>
                    
                    <h3>Dettagli Condivisioni</h3>
                    <div style="max-height: 400px; overflow-y: auto;">`;
            
            for (const [type, links] of Object.entries(userData.links_by_type)) {
                if (links.length > 0) {
                    html += `<h4 style="margin: 20px 0 10px 0; color: #2e5c3e;">${type} (${links.length})</h4>`;
                    links.forEach(link => {
                        html += `
                            <div class="card" style="margin-bottom: 10px; padding: 15px;">
                                <strong>${link.file_name}</strong> 
                                <span class="badge badge-${link.source.toLowerCase()}">${link.source}</span><br>
                                <small>Condiviso con: ${link.shared_with}</small><br>
                                <small>Tipo file: ${link.file_type}</small>
                            </div>`;
                    });
                }
            }
            
            html += `</div></div>`;
            content.innerHTML = html;
            modal.style.display = 'block';
        }
    }
    
    // Modal per dettagli dominio
    function showDomainDetails(domain) {
        const modal = document.getElementById('domainModal');
        const content = document.getElementById('domainModalContent');
        
        if (reportData.domain_details[domain]) {
            const domainData = reportData.domain_details[domain];
            
            let html = `
                <div class="modal-header">
                    <h2>🌐 ${domain}</h2>
                    <span class="close" onclick="closeModal('domainModal')">&times;</span>
                </div>
                <div class="modal-body">
                    <div class="grid">
                        <div class="stat-card">
                            <h3>Totale Condivisioni</h3>
                            <div class="number">${domainData.share_count}</div>
                        </div>
                        <div class="stat-card">
                            <h3>SharePoint</h3>
                            <div class="number">${domainData.sharepoint_shares}</div>
                            <div class="source-badge source-sharepoint">SharePoint</div>
                        </div>
                        <div class="stat-card">
                            <h3>OneDrive</h3>
                            <div class="number">${domainData.onedrive_shares}</div>
                            <div class="source-badge source-onedrive">OneDrive</div>
                        </div>
                        <div class="stat-card">
                            <h3>Livello Rischio</h3>
                            <div class="number">${domainData.risk_level}</div>
                        </div>
                    </div>
                    
                    <h3>Condivisioni</h3>
                    <div style="max-height: 400px; overflow-y: auto;">`;
            
            domainData.shares.forEach(share => {
                html += `
                    <div class="card" style="margin-bottom: 10px; padding: 15px;">
                        <strong>${share.file_name}</strong> 
                        <span class="badge badge-${share.source.toLowerCase()}">${share.source}</span><br>
                        <small>Condiviso da: ${share.shared_by}</small><br>
                        <small>Destinatario: ${share.shared_with}</small>
                    </div>`;
            });
            
            html += `</div></div>`;
            content.innerHTML = html;
            modal.style.display = 'block';
        }
    }
    
    function closeModal(modalId) {
        const modal = document.getElementById(modalId);
        if (modal) modal.style.display = 'none';
    }
    
    // Ricerca tabelle
    function searchTable(inputId, tableId) {
        const input = document.getElementById(inputId);
        const table = document.getElementById(tableId);
        if (!input || !table) return;
        
        const filter = input.value.toUpperCase();
        const rows = table.getElementsByTagName('tr');

        for (let i = 1; i < rows.length; i++) {
            let visible = false;
            const cells = rows[i].getElementsByTagName('td');
            
            for (let j = 0; j < cells.length; j++) {
                if (cells[j].textContent.toUpperCase().indexOf(filter) > -1) {
                    visible = true;
                    break;
                }
            }
            
            rows[i].style.display = visible ? '' : 'none';
        }
    }
    
    // Event listeners
    window.onclick = function(event) {
        const userModal = document.getElementById('userModal');
        const domainModal = document.getElementById('domainModal');
        const siteModal = document.getElementById('siteModal');
        const onedriveModal = document.getElementById('onedriveModal');
        
        if (event.target === userModal) {
            userModal.style.display = 'none';
        }
        if (event.target === domainModal) {
            domainModal.style.display = 'none';
        }
        if (event.target === siteModal) {
            siteModal.style.display = 'none';
        }
        if (event.target === onedriveModal) {
            onedriveModal.style.display = 'none';
        }
    }
    
    // Inizializzazione
    document.addEventListener('DOMContentLoaded', function() {
        const firstTab = document.querySelector('.tab-button');
        const firstContent = document.querySelector('.tab-content');
        if (firstTab) firstTab.classList.add('active');
        if (firstContent) firstContent.classList.add('active');
        
        updateUserTable();
        updateDomainTable();
        updateSiteTable();
        updateOneDriveTable();
        
        setTimeout(initSummaryCharts, 500);
    });
    </script>'''

    # Genera contenuto HTML
    html_content = f'''<!DOCTYPE html>
<html lang="it">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Report Audit condivisioni attive- SharePoint & OneDrive</title>
    {css}
</head>
<body>
    <div class="container">
        <div class="header">
            <div class="header-content">
                <img src="{logo_brainwise_base64}" alt="Logo Brainwise" class="logo">
                <div class="header-text">
                    <h1>Report Audit condivisioni attive</h1>
                    <p>Analisi completa SharePoint & OneDrive - Dashboard Avanzato</p>
                </div>
            </div>
        </div>
        
        <div class="tabs">
            <button class="tab-button" onclick="showTab('summary', event)">📊 Panoramica</button>
            <button class="tab-button" onclick="showTab('sharepoint', event)">🔷 SharePoint</button>
            <button class="tab-button" onclick="showTab('onedrive', event)">☁️ OneDrive</button>
            <button class="tab-button" onclick="showTab('users', event)">👥 Utenti</button>
        </div>
        
        <!-- Tab Panoramica -->
        <div id="summary" class="tab-content">
            <!-- Grid con 4 card: 1 placeholder + 3 con dati -->
            <div class="grid-4">
                <div class="placeholder-card">
                    <img src="{logo_cliente_base64}" alt="Logo Cliente" class="logo">
                </div>
                <div class="stat-card" onclick="showTab('sharepoint')" style="cursor: pointer;">
                    <h3>Totale Condivisioni Attive</h3>
                    <div class="number">{report['total_shares']:,}</div>
                    <div class="trend">SharePoint + OneDrive</div>
                </div>
                <div class="stat-card" onclick="showTab('sharepoint')" style="cursor: pointer;">
                    <h3>Condivisioni SharePoint</h3>
                    <div class="number">{report['sharepoint_shares']:,}</div>
                    <div class="trend">Siti e librerie</div>
                    <div class="source-badge source-sharepoint">SharePoint</div>
                </div>
                <div class="stat-card" onclick="showTab('onedrive')" style="cursor: pointer;">
                    <h3>Condivisioni OneDrive</h3>
                    <div class="number">{report['onedrive_shares']:,}</div>
                    <div class="trend">File personali</div>
                    <div class="source-badge source-onedrive">OneDrive</div>
                </div>
            </div>
            
            <div class="chart-grid">
                <div class="chart-container">
                    <h3>📊 Distribuzione per Sorgente</h3>
                    <canvas id="sourceChart" width="400" height="300"></canvas>
                </div>
                <div class="chart-container">
                    <h3>📈 Timeline Attività</h3>
                    <canvas id="timelineChart" width="400" height="300"></canvas>
                </div>
            </div>
        </div>
        
        <!-- Tab SharePoint -->
        <div id="sharepoint" class="tab-content">
            <div class="grid">
                <div class="stat-card">
                    <h3>Condivisioni SharePoint</h3>
                    <div class="number">{report['sharepoint_shares']:,}</div>
                    <div class="source-badge source-sharepoint">SharePoint</div>
                </div>
                <div class="stat-card">
                    <h3>Siti Unici</h3>
                    <div class="number">{len(report['sharepoint_analysis'].get('by_site', {})):,}</div>
                    <div class="trend">Siti con condivisioni</div>
                </div>
                <div class="stat-card">
                    <h3>Link Senza Scadenza</h3>
                    <div class="number">{report['sharepoint_analysis'].get('never_expires', 0):,}</div>
                    <div class="trend">Potenziale rischio</div>
                </div>
                <div class="stat-card">
                    <h3>Link Protetti</h3>
                    <div class="number">{report['sharepoint_analysis'].get('password_protected', {}).get('True', 0):,}</div>
                    <div class="trend">Con password</div>
                </div>
            </div>
            
            <div class="chart-container clickable-chart">
                <h3>📊 Condivisioni per Sito SharePoint</h3>
                <canvas id="siteChart" width="400" height="300"></canvas>
            </div>
            
            <!-- Sezione Siti -->
            <div class="card">
                <h2>🏢 Analisi Siti SharePoint</h2>
                <input type="text" id="siteSearch" class="search-box" placeholder="🔍 Cerca siti..." onkeyup="searchTable('siteSearch', 'sitesTable')">
                <table id="sitesTable">
                    <thead>
                        <tr>
                            <th onclick="sortTable(document.getElementById('sitesTable'), 0, 'string')">Sito</th>
                            <th onclick="sortTable(document.getElementById('sitesTable'), 1, 'number')">Condivisioni</th>
                            <th onclick="sortTable(document.getElementById('sitesTable'), 2, 'number')">Librerie</th>
                            <th onclick="sortTable(document.getElementById('sitesTable'), 3, 'number')">Link Senza Scadenza</th>
                            <th>Azioni</th>
                        </tr>
                    </thead>
                    <tbody></tbody>
                </table>
            </div>
        </div>
        
        <!-- Tab OneDrive -->
        <div id="onedrive" class="tab-content">
            <div class="grid">
                <div class="stat-card">
                    <h3>Condivisioni OneDrive</h3>
                    <div class="number">{report['onedrive_shares']:,}</div>
                    <div class="source-badge source-onedrive">OneDrive</div>
                </div>
                <div class="stat-card">
                    <h3>Con Scadenza</h3>
                    <div class="number">{report['onedrive_analysis'].get('with_expiration', 0):,}</div>
                    <div class="trend">Link temporanei</div>
                </div>
                <div class="stat-card">
                    <h3>Senza Scadenza</h3>
                    <div class="number">{report['onedrive_analysis'].get('without_expiration', 0):,}</div>
                    <div class="trend">Potenziale rischio</div>
                </div>
                <div class="stat-card">
                    <h3>Link Protetti</h3>
                    <div class="number">{report['onedrive_analysis'].get('password_protected', {}).get('True', 0):,}</div>
                    <div class="trend">Con password</div>
                </div>
            </div>
            
            <div class="chart-grid">
                <div class="chart-container">
                    <h3>📊 Distribuzione Permessi OneDrive</h3>
                    <canvas id="permissionChart" width="400" height="300"></canvas>
                </div>
                <div class="chart-container">
                    <h3>🔒 Analisi Sicurezza OneDrive</h3>
                    <canvas id="securityChart" width="400" height="300"></canvas>
                </div>
            </div>
            
            <!-- Sezione Account OneDrive -->
            <div class="card">
                <h2>☁️ Analisi Account OneDrive</h2>
                <input type="text" id="onedriveSearch" class="search-box" placeholder="🔍 Cerca account OneDrive..." onkeyup="searchTable('onedriveSearch', 'onedriveTable')">
                <table id="onedriveTable">
                    <thead>
                        <tr>
                            <th onclick="sortTable(document.getElementById('onedriveTable'), 0, 'string')">Account</th>
                            <th onclick="sortTable(document.getElementById('onedriveTable'), 1, 'number')">Condivisioni</th>
                            <th onclick="sortTable(document.getElementById('onedriveTable'), 2, 'number')">Con Scadenza</th>
                            <th onclick="sortTable(document.getElementById('onedriveTable'), 3, 'number')">Senza Scadenza</th>
                            <th onclick="sortTable(document.getElementById('onedriveTable'), 4, 'number')">Protetti</th>
                            <th>Azioni</th>
                        </tr>
                    </thead>
                    <tbody></tbody>
                </table>
            </div>
        </div>
        
<!-- Tab Utenti -->
<div id="users" class="tab-content">
    <!-- Tile Legenda Risk Score -->
    <div class="card legend-card">
        <h3>📊 Legenda Risk Score</h3>
        <p class="legend-description">Indica il livello di rischio associato alle condivisioni effettuate:</p>
        <div class="legend-grid">
            <div class="legend-item risk-high">
                <span class="risk-indicator">🔴</span>
                <div class="risk-info">
                    <strong>High</strong>
                    <span>Rischio molto alto – numerose condivisioni potenzialmente critiche</span>
                </div>
            </div>
            <div class="legend-item risk-medium">
                <span class="risk-indicator">🟠</span>
                <div class="risk-info">
                    <strong>Medium</strong>
                    <span>Rischio medio – attività da monitorare</span>
                </div>
            </div>
            <div class="legend-item risk-low">
                <span class="risk-indicator">🟢</span>
                <div class="risk-info">
                    <strong>Low</strong>
                    <span>Rischio basso – attività contenute</span>
                </div>
            </div>
        </div>
    </div>
    
    <!-- Tabella Utenti -->
    <div class="card">
        <h2>👥 Analisi Utenti</h2>
        <input type="text" id="userSearch" class="search-box" placeholder="🔍 Cerca utenti..." onkeyup="searchTable('userSearch', 'usersTable')">
        <table id="usersTable">
            <thead>
                <tr>
                    <th onclick="sortTable(document.getElementById('usersTable'), 0, 'string')">Utente</th>
                    <th onclick="sortTable(document.getElementById('usersTable'), 1, 'number')">Totale</th>
                    <th onclick="sortTable(document.getElementById('usersTable'), 2, 'number')">SharePoint</th>
                    <th onclick="sortTable(document.getElementById('usersTable'), 3, 'number')">OneDrive</th>
                    <th onclick="sortTable(document.getElementById('usersTable'), 4, 'number')">Risk Score</th>
                    <th onclick="sortTable(document.getElementById('usersTable'), 5, 'number')">Domini Esterni</th>
                    <th>Azioni</th>
                </tr>
            </thead>
            <tbody></tbody>
        </table>
    </div>
</div>
        
        <!-- Tab Domini -->
        <div id="domains" class="tab-content">
            <div class="card">
                <h2>🌐 Analisi Domini</h2>
                <input type="text" id="domainSearch" class="search-box" placeholder="🔍 Cerca domini..." onkeyup="searchTable('domainSearch', 'domainsTable')">
                <table id="domainsTable">
                    <thead>
                        <tr>
                            <th>Dominio</th>
                            <th>Totale</th>
                            <th>SharePoint</th>
                            <th>OneDrive</th>
                            <th>Livello Rischio</th>
                            <th>Azioni</th>
                        </tr>
                    </thead>
                    <tbody></tbody>
                </table>
            </div>
        </div>
        
        <div class="footer-disclaimer">
            <p><strong>Disclaimer:</strong> Questo report è stato generato senza condividere o elaborare dati del cliente tramite strumenti AI.</p>
        </div>
    </div>
    
    <!-- Modal Dettagli Sito -->
    <div id="siteModal" class="modal">
        <div class="modal-content">
            <div id="siteModalContent"></div>
        </div>
    </div>
    
    <!-- Modal Dettagli Account OneDrive -->
    <div id="onedriveModal" class="modal">
        <div class="modal-content">
            <div id="onedriveModalContent"></div>
        </div>
    </div>
    
    <!-- Modal Dettagli Utente -->
    <div id="userModal" class="modal">
        <div class="modal-content">
            <div id="userModalContent"></div>
        </div>
    </div>
    
    <!-- Modal Dettagli Dominio -->
    <div id="domainModal" class="modal">
        <div class="modal-content">
            <div id="domainModalContent"></div>
        </div>
    </div>
    
    ''' + js + '''
</body>
</html>'''

    # Scrivi il file
    with open(output_path, 'w', encoding='utf-8') as f:
        f.write(html_content)

def main():
    root = tk.Tk()
    root.withdraw()
    
    # Seleziona file SharePoint
    messagebox.showinfo("Selezione File", "Seleziona il file CSV di SharePoint")
    spo_file = filedialog.askopenfilename(
        title="Seleziona Report SharePoint CSV", 
        filetypes=[("CSV", "*.csv")]
    )
    if not spo_file:
        messagebox.showinfo("Operazione Annullata", "Nessun file SharePoint selezionato.")
        return
    
    # Seleziona file OneDrive
    messagebox.showinfo("Selezione File", "Seleziona il file CSV di OneDrive")
    od_file = filedialog.askopenfilename(
        title="Seleziona Report OneDrive CSV", 
        filetypes=[("CSV", "*.csv")]
    )
    if not od_file:
        messagebox.showinfo("Operazione Annullata", "Nessun file OneDrive selezionato.")
        return
    
    # Selezione logo Brainwise (opzionale)
    messagebox.showinfo("Selezione Logo Brainwise", "Seleziona il logo Brainwise (se vuoi usare quello di default premi Annulla)")
    logo_brainwise_path = filedialog.askopenfilename(
        title="Seleziona Logo Brainwise", 
        filetypes=[
            ("File immagine", "*.png *.jpg *.jpeg *.gif *.svg"),
            ("Tutti i file", "*.*")
        ]
    )
    if not logo_brainwise_path:
        logo_brainwise_path = None

    # Selezione logo Cliente (opzionale)
    messagebox.showinfo("Selezione Logo Cliente", "Seleziona il logo del cliente (verrà mostrato nella tile a sinistra, premi Annulla per usare quello di default)")
    logo_cliente_path = filedialog.askopenfilename(
        title="Seleziona Logo Cliente", 
        filetypes=[
            ("File immagine", "*.png *.jpg *.jpeg *.gif *.svg"),
            ("Tutti i file", "*.*")
        ]
    )
    if not logo_cliente_path:
        messagebox.showwarning("Attenzione", "Nessun logo cliente selezionato. Verrà utilizzato il logo di default.")
        logo_cliente_path = None

    try:
        # Carica i dati con gestione dell'encoding
        print(f"Caricamento SharePoint: {spo_file}")
        df_spo = pd.read_csv(spo_file, encoding='utf-8-sig', on_bad_lines='skip')
        print(f"SharePoint caricato: {len(df_spo)} righe")
        
        print(f"Caricamento OneDrive: {od_file}")
        df_od = pd.read_csv(od_file, encoding='utf-8-sig', on_bad_lines='skip')
        print(f"OneDrive caricato: {len(df_od)} righe")
        
        messagebox.showinfo("Elaborazione", "Analisi dei dati e generazione del report avanzato...")
        
        # Analizza i dati
        report = analyze_combined_sharing(df_spo, df_od)
        
        # Salva il report
        save_path = filedialog.asksaveasfilename(
            title="Salva Report HTML", 
            defaultextension=".html", 
            filetypes=[("HTML", "*.html")]
        )
        if not save_path:
            messagebox.showinfo("Operazione Annullata", "Report non salvato.")
            return
        
        # Genera il report HTML con due loghi
        generate_combined_html_report(report, save_path, logo_brainwise_path, logo_cliente_path)
        messagebox.showinfo("Successo", f"Report avanzato salvato in: {save_path}")
        
    except Exception as e:
        messagebox.showerror("Errore", f"Errore durante l'elaborazione: {str(e)}")

if __name__ == '__main__':
    main()