SELECT p.id, p.product_name, p.launch_date FROM products p WHERE p.id IN (SELECT product_id FROM store_sales WHERE revenue <= 3600) ORDER BY id DESC;
