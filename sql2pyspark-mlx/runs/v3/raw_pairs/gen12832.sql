SELECT launch_date FROM products WHERE price <= 970 EXCEPT SELECT order_date FROM orders;
