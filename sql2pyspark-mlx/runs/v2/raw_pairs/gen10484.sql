SELECT id, started_at, customer_id, country, HOUR(started_at) AS started_at_hour FROM sessions WHERE browser = 'Firefox' AND customer_id IS NOT NULL;
