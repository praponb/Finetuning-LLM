SELECT id, started_at, country, customer_id, DATE_TRUNC('YEAR', started_at) AS started_at_period FROM sessions ORDER BY started_at DESC
