SELECT launch_date FROM products WHERE launch_date IS NOT NULL UNION SELECT order_date FROM orders WHERE order_date IS NOT NULL
