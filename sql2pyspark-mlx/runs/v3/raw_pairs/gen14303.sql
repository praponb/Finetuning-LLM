SELECT
    status,
    CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(channel))) channel_values,
    ROUND((100 * SUM(CASE WHEN channel = 'partner' THEN 1 ELSE 0 END)) / COUNT(*), 1) pct_partner,
    ROUND(AVG(shipping_fee), 2) avg_shipping_fee
FROM orders
WHERE customer_id = 36
GROUP BY status
ORDER BY avg_shipping_fee DESC, channel_values DESC
