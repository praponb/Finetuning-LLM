SELECT
    i.id,
    i.store_id,
    i.product_id,
    i.quantity
FROM inventory i
WHERE i.quantity <= 161 AND i.product_id IN (SELECT id FROM products WHERE cost BETWEEN 190 AND 350);
