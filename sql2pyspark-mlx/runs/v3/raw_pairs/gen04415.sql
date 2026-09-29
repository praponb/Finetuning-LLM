SELECT country, browser, device, TRIM(browser) AS clean_browser FROM sessions WHERE browser IN ('Firefox', 'Edge', 'Chrome');
