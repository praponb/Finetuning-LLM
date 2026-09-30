SELECT id, customer_id, started_at, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY started_at DESC, id) AS row_num FROM sessions WHERE device = 'tablet';
