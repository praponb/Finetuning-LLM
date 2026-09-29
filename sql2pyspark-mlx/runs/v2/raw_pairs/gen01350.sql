SELECT id, customer_id, started_at, PERCENT_RANK() OVER (PARTITION BY customer_id ORDER BY started_at) AS started_at_pct_rank FROM sessions WHERE browser = 'Safari';
