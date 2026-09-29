SELECT ss.id, ss.product_id, ss.sale_date, ss.units FROM store_sales ss WHERE ss.product_id IN (SELECT id FROM products WHERE price BETWEEN 480 AND 970) ORDER BY sale_date;
