SELECT device, browser, country, INITCAP(TRIM(browser)) AS browser_title FROM sessions WHERE browser != 'Edge'
