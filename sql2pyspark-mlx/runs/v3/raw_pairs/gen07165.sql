SELECT p.id, p.launch_date FROM products p WHERE NOT EXISTS (SELECT 1 FROM store_sales ss WHERE ss.product_id = p.id AND ss.sale_date < '2023-08-01')
