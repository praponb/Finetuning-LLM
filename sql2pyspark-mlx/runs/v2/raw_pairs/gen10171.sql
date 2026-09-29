SELECT channel, ROUND(STDDEV(discount), 2) AS stddev_discount, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(status))) AS status_values
FROM orders
WHERE channel = 'partner'
GROUP BY channel
HAVING ROUND(STDDEV(discount), 2) < 50;
