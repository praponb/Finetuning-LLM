SELECT *, CONCAT(browser, '/', country) AS browser_country, TRIM(device) AS clean_device FROM sessions;
