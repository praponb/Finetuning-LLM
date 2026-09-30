SELECT o.id, o.status, COUNT(p.id) AS payment_count
FROM orders o
INNER JOIN payments p ON o.id = p.order_id
GROUP BY o.id, o.status
ORDER BY status, o.id DESC
LIMIT 1
