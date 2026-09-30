SELECT o.id, o.store_id, o.amount
FROM orders o
WHERE o.store_id IN (SELECT id FROM stores WHERE city NOT IN ('New York', 'Bangkok'))
ORDER BY o.id DESC
