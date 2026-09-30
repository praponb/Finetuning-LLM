SELECT p.id, p.supplier_id
FROM products p
WHERE EXISTS (SELECT 1 FROM store_sales ss WHERE ss.product_id = p.id AND ss.revenue < 2900)
