SELECT p.id, p.order_id, o.status
FROM payments p
JOIN orders o ON p.order_id = o.id
WHERE p.order_id = 16
ORDER BY id DESC;
