SELECT order_date FROM orders WHERE MONTH(order_date) IN (3, 6, 8) UNION SELECT last_restock order_date FROM inventory ORDER BY order_date;
