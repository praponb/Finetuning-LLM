SELECT o.status, COUNT(p.id) AS payment_count
FROM orders o
LEFT JOIN payments p ON p.order_id = o.id
WHERE p.status NOT IN ('FAILED', 'SUCCESS')
GROUP BY o.status
HAVING payment_count >= 5
