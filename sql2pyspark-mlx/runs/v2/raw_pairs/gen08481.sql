SELECT ss.id, ss.store_id FROM store_sales ss WHERE ss.sale_date <= '2024-08-01' AND ss.product_id IN (SELECT id FROM products WHERE category = 'Home') ORDER BY id LIMIT 1;
