SELECT status, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(channel))) AS channel_values, MIN(order_date) AS min_order_date
FROM orders
WHERE channel NOT IN ('mobile', 'partner') AND discount IS NOT NULL
GROUP BY status
ORDER BY channel_values;
