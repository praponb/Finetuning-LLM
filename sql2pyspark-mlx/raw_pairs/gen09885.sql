SELECT order_date FROM orders WHERE shipping_fee >= 25 UNION SELECT launch_date FROM products ORDER BY order_date;
