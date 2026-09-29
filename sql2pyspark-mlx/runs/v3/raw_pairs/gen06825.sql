SELECT id, country, browser, UPPER(browser) AS browser_upper FROM sessions WHERE started_at >= '2024-01-01'
