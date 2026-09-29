SELECT ss.id, ss.store_id, ss.revenue, ss.product_id FROM store_sales ss WHERE ss.units IS NOT NULL AND ss.product_id IN (SELECT id FROM products WHERE price BETWEEN 390 AND 660) ORDER BY id;
