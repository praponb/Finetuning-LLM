SELECT order_date FROM orders UNION ALL SELECT launch_date AS order_date FROM products ORDER BY order_date;
