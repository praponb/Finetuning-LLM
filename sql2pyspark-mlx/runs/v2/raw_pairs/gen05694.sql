SELECT
    status,
    SUM(CASE WHEN status = 'CANCELLED' THEN 1 ELSE 0 END) AS cancelled_count,
    CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(channel))) AS channel_values,
    MAX(shipping_fee) - MIN(shipping_fee) AS shipping_fee_range
FROM orders
WHERE customer_id = 35
GROUP BY status
HAVING SUM(CASE WHEN status = 'CANCELLED' THEN 1 ELSE 0 END) < 70;
