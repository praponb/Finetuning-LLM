SELECT i.id, i.store_id
FROM inventory i
WHERE i.quantity BETWEEN 114 AND 239 AND i.store_id IN (SELECT id FROM stores WHERE city IN ('Berlin', 'London', 'Bangkok'))
ORDER BY i.id DESC
