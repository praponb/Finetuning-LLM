SELECT
    i.id,
    i.quantity,
    i.last_restock,
    i.store_id
FROM inventory i
WHERE i.quantity > 125 AND i.store_id NOT IN (SELECT id FROM stores WHERE city IN ('Chiang Mai', 'Bangkok', 'Singapore'))
