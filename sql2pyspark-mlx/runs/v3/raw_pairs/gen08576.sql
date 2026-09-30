SELECT
    i.id,
    i.quantity,
    i.store_id,
    i.last_restock
FROM inventory i
WHERE i.product_id IN (SELECT id FROM products WHERE brand = 'Initech');
