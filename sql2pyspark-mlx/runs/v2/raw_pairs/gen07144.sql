SELECT
    status,
    COUNT(*) AS num_orders,
    SORT_ARRAY(COLLECT_SET(channel)) AS channel_list,
    MAX(shipping_fee) - MIN(shipping_fee) AS shipping_fee_range
FROM orders
WHERE amount > 1000 AND shipping_fee BETWEEN 23 AND 32
GROUP BY status;
