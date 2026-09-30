SELECT p.id, p.supplier_id, p.brand, p.product_name FROM products p WHERE EXISTS (SELECT 1 FROM store_sales ss WHERE ss.product_id = p.id AND ss.store_id IN (2, 6)) ORDER BY p.id DESC;
