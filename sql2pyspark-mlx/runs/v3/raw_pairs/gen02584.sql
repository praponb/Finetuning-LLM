SELECT id, country, browser, device, DATE_TRUNC('MONTH', started_at) AS started_at_period FROM sessions ORDER BY browser, device, id LIMIT 1
