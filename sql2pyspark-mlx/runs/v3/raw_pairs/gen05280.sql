SELECT
    id,
    customer_id,
    started_at,
    device,
    ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY started_at, id) AS row_num,
    NTILE(2) OVER (PARTITION BY device ORDER BY started_at DESC, id) AS quartile
FROM sessions
