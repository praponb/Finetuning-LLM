SELECT p.id, p.status, o.customer_id
FROM payments p
INNER JOIN orders o ON p.order_id = o.id
ORDER BY o.customer_id
