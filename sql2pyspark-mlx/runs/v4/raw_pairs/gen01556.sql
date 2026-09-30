SELECT p.id, p.product_name, p.launch_date, p.cost FROM products p WHERE p.id IN (SELECT product_id FROM store_sales WHERE store_id = 2)
