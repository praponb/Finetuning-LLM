SELECT
    id,
    country,
    customer_id,
    started_at,
    browser,
    device,
    NTILE(3) OVER (PARTITION BY country, customer_id ORDER BY started_at DESC, id) bucket,
    LAST_VALUE(device) OVER (PARTITION BY browser ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) last_device
FROM sessions;
