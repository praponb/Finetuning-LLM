SELECT p.id, p.supplier_id, p.product_name
FROM products p
WHERE EXISTS (SELECT 1 FROM store_sales ss WHERE ss.product_id = p.id)
ORDER BY supplier_id DESC;
