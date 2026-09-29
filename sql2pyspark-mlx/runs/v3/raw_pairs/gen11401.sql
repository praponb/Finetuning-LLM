SELECT p.id, p.supplier_id, p.brand, p.price FROM products p WHERE EXISTS (SELECT 1 FROM store_sales ss WHERE ss.product_id = p.id AND ss.product_id IN (3, 4))
