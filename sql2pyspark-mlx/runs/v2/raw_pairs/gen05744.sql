SELECT id, country, INITCAP(TRIM(browser)) AS browser_title, TRIM(device) AS clean_device FROM sessions WHERE browser = 'Firefox'
