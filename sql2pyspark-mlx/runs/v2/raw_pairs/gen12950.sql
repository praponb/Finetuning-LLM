SELECT
    YEAR(order_date) order_year,
    SUM(CASE WHEN channel = 'web' THEN 1 ELSE 0 END) web_count,
    CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(channel))) channel_values,
    COUNT(*) num_orders
FROM orders
WHERE shipping_fee IS NOT NULL
GROUP BY order_year
HAVING web_count < 41
