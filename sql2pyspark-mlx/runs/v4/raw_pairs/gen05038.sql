SELECT id, status, customer_id, opened_date, NTILE(3) OVER (PARTITION BY status, customer_id ORDER BY opened_date, id) AS quartile FROM accounts ORDER BY id;
