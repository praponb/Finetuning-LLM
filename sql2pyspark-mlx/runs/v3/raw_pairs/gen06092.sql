SELECT p.id, p.amount
FROM payments p
WHERE p.order_id IN (SELECT id FROM orders WHERE discount BETWEEN 0.19 AND 0.25)
ORDER BY p.amount DESC, id ASC
LIMIT 5
