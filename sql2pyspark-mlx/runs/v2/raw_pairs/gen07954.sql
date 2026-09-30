SELECT p.id, p.price
FROM products p
WHERE p.id IN (SELECT product_id FROM inventory);
