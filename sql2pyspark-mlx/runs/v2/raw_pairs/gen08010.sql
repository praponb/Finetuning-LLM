SELECT
    i.quantity,
    i.store_id,
    i.last_restock,
    p.cost,
    p.brand
FROM inventory i
INNER JOIN products p ON p.id = i.product_id
WHERE i.product_id IN (2, 6, 9, 10)
