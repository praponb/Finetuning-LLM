SELECT order_date FROM orders WHERE status != 'COMPLETED' UNION SELECT opened_date FROM stores ORDER BY order_date DESC;
