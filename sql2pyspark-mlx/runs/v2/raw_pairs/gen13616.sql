SELECT o.status, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(p.status))) AS status_values, SUM(CASE WHEN p.status = 'REFUNDED' THEN p.amount ELSE 0 END) AS refunded_amount
FROM payments p
RIGHT JOIN orders o ON o.id = p.order_id
FULL OUTER JOIN customers c ON o.customer_id = c.id
GROUP BY o.status
