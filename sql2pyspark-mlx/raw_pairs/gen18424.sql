SELECT id, started_at, customer_id, DENSE_RANK() OVER (PARTITION BY DATE_TRUNC('MONTH', started_at), customer_id ORDER BY started_at) AS started_at_dense_rank FROM sessions;
