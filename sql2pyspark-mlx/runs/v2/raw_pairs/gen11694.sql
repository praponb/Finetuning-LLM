SELECT status, method, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(status))) AS status_values
FROM payments
WHERE order_id IS NOT NULL AND order_id IN (4, 12) AND order_id IN (19, 27, 40)
GROUP BY ROLLUP(status, method)
ORDER BY 2 DESC
