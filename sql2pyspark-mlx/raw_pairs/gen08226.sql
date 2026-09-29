SELECT id, device, browser, UPPER(country) AS country_upper FROM sessions WHERE country NOT IN ('TH', 'SG', 'UK')
