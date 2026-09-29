SELECT o.store_id, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(p.method))) AS method_values, ROUND((100 * SUM(CASE WHEN p.status = 'SUCCESS' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_success
FROM payments p
INNER JOIN orders o ON p.order_id = o.id
WHERE p.method IN ('paypal', 'cash', 'card')
GROUP BY o.store_id
ORDER BY 3 DESC
