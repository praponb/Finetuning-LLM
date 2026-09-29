SELECT p.id, p.brand, p.cost
FROM products p
WHERE p.id IN (SELECT product_id FROM store_sales WHERE product_id IN (4, 7));
