SELECT
    channel,
    status,
    COUNT(*) AS num_orders,
    MIN(order_date) AS min_order_date,
    SORT_ARRAY(COLLECT_SET(channel)) AS channel_list
FROM orders
WHERE store_id IN (20, 28, 37, 40)
GROUP BY channel, status
ORDER BY status, channel
LIMIT 1;
