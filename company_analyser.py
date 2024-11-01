import os
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
import pickle
import requests
import json
import time
from typing import Dict, Any
import argparse

class CompanyAnalyzer:
    def __init__(self, pplx_api_key: str, spreadsheet_id: str):
        self.pplx_api_key = pplx_api_key
        self.spreadsheet_id = spreadsheet_id
        self.sheets_service = self._initialize_sheets_service()

    def _initialize_sheets_service(self):
        """Initialize Google Sheets API service."""
        SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
        creds = None
        
        if os.path.exists('token.pickle'):
            with open('token.pickle', 'rb') as token:
                creds = pickle.load(token)
                
        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                creds.refresh(Request())
            else:
                flow = InstalledAppFlow.from_client_secrets_file(
                    'credentials.json', SCOPES)
                creds = flow.run_local_server(port=0)
            with open('token.pickle', 'wb') as token:
                pickle.dump(creds, token)
                
        return build('sheets', 'v4', credentials=creds)

    def get_company_data(self, start_row: int, end_row: int) -> list:
        """Fetch company names and domains from Google Sheet for specified rows."""
        range_name = f'A{start_row}:B{end_row}'
        result = self.sheets_service.spreadsheets().values().get(
            spreadsheetId=self.spreadsheet_id,
            range=range_name,
        ).execute()
        return result.get('values', [])

    def call_pplx_api(self, domain: str) -> Dict[str, Any]:
        """Call PPLX API with the given domain."""
        headers = {
            "Authorization": f"Bearer {self.pplx_api_key}",
            "Content-Type": "application/json"
        }
        
        # Exact prompt as provided
        prompt = f'''Please scrape the web and provide the latest verified information about the following company:
Website/Domain: {domain}

Company Overview: Provide a brief description of the company (up to 200 words), including: What the company does, the industry it operates in, and any recent significant developments or news related to the company
Company Type analysis: Based on available content, classify the company as either:

'Product-based': Companies that offer a platform, software, tool, or tangible product that customers can use independently (including digital platforms/apps where customers interact through the company's proprietary system)
'Service-based': Companies that primarily deliver human-performed services, consulting, or custom solutions that require direct company involvement for each customer interaction

Note: If a company offers a technology platform or software solution through which customers can self-serve or interact, it should be classified as product-based, even if there are supporting services involved.

Market Classification: Using the same sources, determine whether the company operates in: 'B2B', 'B2C', 'D2C', other relevant categories.
Industry: Using verified sources, identify the primary industry classification.

Sources: For reliable results, please ensure that the information is cross-verified against credible sources such as the company's official website, industry reports, verified news articles, LinkedIn profiles, Crunchbase, or other reputable business directories. Share the Crunchbase source that was accessed. 

Return the result strictly in JSON format only, and nothing else, as shown below:
{{
"website": "Flexiple.com",
"company_overview": "Flexiple is the simplest & fastest way to build your dream tech team. Simply share your talent requirements and receive handpicked candidates in your inbox in 48 hours. Access pre-vetted quality engineers: Get direct access to Flexiple's talent who are carefully vetted over 50+ unique data points parameterized based on past work andcrowdsourced from their performance on hiring processes through Flexiple.",
"company_type": "Service-based",
"company_business": "B2B",
"company_industry": "IT Consulting & IT services"
"sources": "https://www.crunchbase.com/organization/flexiple"
}}'''

        payload = {
            "model": "llama-3.1-70b-instruct",
            "messages": [{"role": "user", "content": prompt}]
        }

        try:
            response = requests.post(
                "https://api.perplexity.ai/chat/completions",
                headers=headers,
                json=payload,
                timeout=30
            )
            
            if response.status_code == 200:
                try:
                    content = response.json()['choices'][0]['message']['content']
                    
                    # Clean up the content
                    # Remove markdown code blocks if present
                    content = content.replace('```json', '').replace('```', '')
                    
                    # Remove "Here is the result in JSON format:" and similar prefixes
                    content = content.split('{', 1)[-1]
                    content = '{' + content
                    
                    # Find the last closing brace
                    if '}' in content:
                        content = content[:content.rindex('}')+1]
                    
                    # Try to parse the cleaned JSON
                    try:
                        result = json.loads(content)
                        print(f"Successfully parsed JSON for {domain}")
                        return result
                    except json.JSONDecodeError as e:
                        print(f"Error parsing cleaned JSON for {domain}: {e}")
                        print(f"Cleaned content: {content[:500]}...")
                        return None
                    
                except (json.JSONDecodeError, KeyError) as e:
                    print(f"Error parsing response for {domain}: {e}")
                    print(f"Raw response: {response.text[:1000]}...")
                    return None
            else:
                print(f"API call failed for {domain}: {response.status_code}")
                print(f"Error response: {response.text}")
                return None
                
        except requests.exceptions.RequestException as e:
            print(f"Request failed for {domain}: {e}")
            return None

    def update_sheet_with_response(self, row: int, response: Dict[str, Any]):
        """Update Google Sheet with API response using correct column mappings."""
        if not response:
            return
            
        # Map the API response fields to sheet columns
        values = [[
            response.get('company_overview', ''),  # companyOverview
            response.get('company_type', ''),      # companyType
            response.get('company_business', ''),  # companyBusiness
            response.get('company_industry', ''),  # companyIndustry
            response.get('sources', '')            # companyCrunchbase
        ]]
        
        # Update the sheet with mapped values
        range_name = f'C{row}:G{row}'  # Assuming columns C-G are for the responses
        
        body = {
            'values': values
        }
        
        try:
            self.sheets_service.spreadsheets().values().update(
                spreadsheetId=self.spreadsheet_id,
                range=range_name,
                valueInputOption='RAW',
                body=body
            ).execute()
            print(f"Successfully updated row {row}")
        except Exception as e:
            print(f"Error updating row {row}: {e}")

    def process_companies(self, start_row: int, end_row: int):
        """Process companies in the specified row range."""
        print(f"Processing companies from row {start_row} to {end_row}")
        companies = self.get_company_data(start_row, end_row)
        
        for i, company in enumerate(companies, start=start_row):
            try:
                if len(company) < 2:
                    print(f"Skipping row {i}: Incomplete data")
                    continue
                    
                _, domain = company
                print(f"\nProcessing {domain} (Row {i})...")
                
                response = self.call_pplx_api(domain)
                if response:
                    self.update_sheet_with_response(i, response)
                    print(f"Successfully processed {domain}")
                
                # Rate limiting
                time.sleep(1)
                
            except Exception as e:
                print(f"Error processing row {i} ({domain if 'domain' in locals() else 'unknown'}): {e}")
                continue

def main():
    parser = argparse.ArgumentParser(description='Process company domains from Google Sheet')
    parser.add_argument('--start', type=int, help='Starting row number (default: 2)', default=2)
    parser.add_argument('--end', type=int, help='Ending row number')
    parser.add_argument('--batch', type=int, help='Number of entries to process')
    
    args = parser.parse_args()
    
    # Configuration
    PPLX_API_KEY = "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"  # Replace with your actual API key
    SPREADSHEET_ID = "1l-UhapswktHWkFPzbs2Yr2KV3li1uooL9jApj7Hq8LM"  # Replace with your actual spreadsheet ID
    
    if args.batch and not args.end:
        args.end = args.start + args.batch - 1
    
    if not args.end:
        print("Error: Please specify either --end or --batch")
        return
    
    print(f"Starting process for rows {args.start} to {args.end}")
    
    analyzer = CompanyAnalyzer(PPLX_API_KEY, SPREADSHEET_ID)
    analyzer.process_companies(args.start, args.end)
    
    print("Processing completed!")

if __name__ == "__main__":
    main()