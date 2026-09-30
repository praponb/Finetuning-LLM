SELECT
    id,
    customer_id,
    started_at,
    device,
    LAST_VALUE(device) OVER (PARTITION BY customer_id ORDER BY started_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) last_device
FROM sessions
