SELECT id, customer_id, started_at FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY started_at DESC, id) AS pos FROM sessions) AS ranked WHERE pos <= 3
