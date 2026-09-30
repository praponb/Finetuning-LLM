WITH ranked AS (SELECT *, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY started_at, id) AS rn FROM sessions) SELECT id, customer_id, started_at, browser FROM ranked WHERE rn <= 5
