SELECT p.id, p.price
FROM products p
WHERE p.supplier_id = 24 AND NOT EXISTS (SELECT 1 FROM store_sales ss WHERE ss.product_id = p.id);
