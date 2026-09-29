SELECT device, started_at, country, UPPER(browser) AS browser_upper FROM sessions ORDER BY browser_upper DESC, started_at DESC;
