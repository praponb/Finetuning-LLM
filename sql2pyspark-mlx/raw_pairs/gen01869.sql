SELECT p.id, p.cost, p.product_name, p.brand FROM products p WHERE NOT EXISTS (SELECT 1 FROM store_sales ss WHERE ss.product_id = p.id AND ss.product_id IN (5, 6))
