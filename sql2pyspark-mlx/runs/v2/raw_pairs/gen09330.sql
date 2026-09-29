SELECT id, browser, device, TRIM(browser) AS clean_browser, DATE_TRUNC('month', started_at) AS started_at_period FROM sessions ORDER BY 4 DESC
