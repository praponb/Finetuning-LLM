SELECT id, customer_id, started_at, NTILE(3) OVER (PARTITION BY customer_id ORDER BY started_at, id) started_at_ntile FROM sessions WHERE browser = 'Safari';
