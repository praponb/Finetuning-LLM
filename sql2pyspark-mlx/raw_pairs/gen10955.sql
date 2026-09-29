SELECT id, browser, DATE_TRUNC('month', started_at) AS started_at_period FROM sessions ORDER BY 2 ASC;
