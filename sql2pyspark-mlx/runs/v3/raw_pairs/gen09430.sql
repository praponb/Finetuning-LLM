SELECT ss.id, ss.revenue FROM store_sales ss WHERE ss.product_id NOT IN (SELECT id FROM products WHERE brand = 'Stark') ORDER BY ss.id DESC;
