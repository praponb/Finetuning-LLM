SELECT id, browser, DATE_TRUNC('week', started_at) AS started_at_period
FROM sessions
WHERE country != 'TH'
