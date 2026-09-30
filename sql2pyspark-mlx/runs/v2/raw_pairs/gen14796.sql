SELECT
    id,
    store_id,
    status,
    LENGTH(channel) AS channel_length
FROM orders
WHERE discount > 0.35 AND customer_id = 2
ORDER BY status, id DESC
