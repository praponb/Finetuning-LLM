SELECT ss.id, ss.revenue, ss.product_id FROM store_sales ss WHERE ss.product_id IN (SELECT id FROM products WHERE brand = 'Initech');
