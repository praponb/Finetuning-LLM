SELECT c.segment, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(p.status))) status_values
FROM payments p
INNER JOIN orders o ON p.order_id = o.id
LEFT JOIN customers c ON o.customer_id = c.id
WHERE o.discount BETWEEN 0.27 AND 0.34
GROUP BY c.segment;
