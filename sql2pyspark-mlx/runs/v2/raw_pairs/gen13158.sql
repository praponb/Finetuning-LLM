SELECT
    store_id,
    status,
    channel,
    ROUND(STDDEV(discount), 2) AS stddev_discount,
    COUNT(DISTINCT amount) AS unique_channels
FROM orders
GROUP BY store_id, status, channel
HAVING ROUND(STDDEV(discount), 2) >= 27
ORDER BY unique_channels, store_id, status, channel
LIMIT 20
