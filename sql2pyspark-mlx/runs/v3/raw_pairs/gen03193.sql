SELECT id, customer_id, started_at, DENSE_RANK() OVER (PARTITION BY customer_id ORDER BY started_at DESC) dense_rnk FROM sessions WHERE customer_id IN (8, 15, 39)
