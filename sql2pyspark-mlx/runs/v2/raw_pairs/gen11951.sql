SELECT id, order_date FROM orders WHERE order_date = (SELECT MAX(order_date) FROM orders WHERE customer_id IS NOT NULL) ORDER BY order_date ASC, id DESC LIMIT 10
