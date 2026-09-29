SELECT ss.id, ss.product_id FROM store_sales ss WHERE ss.store_id IN (2, 4, 6) AND ss.product_id IN (SELECT id FROM products WHERE price <= 1160);
