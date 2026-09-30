SELECT p.id, p.category FROM products p WHERE NOT EXISTS (SELECT 1 FROM store_sales ss WHERE ss.product_id = p.id AND ss.store_id IN (1, 2, 3, 6))
