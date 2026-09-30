SELECT
    store_id,
    channel,
    order_date,
    TRIM(channel) AS clean_channel
FROM orders
WHERE order_date > '2025-10-01'
ORDER BY store_id DESC
