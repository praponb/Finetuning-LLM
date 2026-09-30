SELECT p.launch_date, ss.revenue FROM products p LEFT JOIN store_sales ss ON p.id = ss.product_id ORDER BY launch_date DESC
