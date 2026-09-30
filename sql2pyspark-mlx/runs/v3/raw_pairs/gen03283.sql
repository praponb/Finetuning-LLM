SELECT status, channel, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(status))) AS status_values
FROM orders
WHERE order_date < '2024-05-11' AND channel NOT IN ('mobile', 'store')
GROUP BY status, channel
HAVING MIN(shipping_fee) >= 24
ORDER BY channel DESC, status
LIMIT 3;
