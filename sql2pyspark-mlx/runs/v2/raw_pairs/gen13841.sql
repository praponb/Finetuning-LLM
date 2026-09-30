SELECT o.status, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(p.method))) AS method_values, ROUND(STDDEV(p.amount), 2) AS stddev_amount
FROM payments p
LEFT JOIN orders o ON o.id = p.order_id
GROUP BY o.status
ORDER BY o.status;
