SELECT
    status,
    CASE WHEN amount >= 1300 THEN 'high' WHEN amount >= 650 THEN 'mid' ELSE 'low' END amount_bucket,
    CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(status))) status_values,
    MAX(shipping_fee) - MIN(shipping_fee) shipping_fee_range
FROM orders
GROUP BY 1, 2
ORDER BY status DESC;
