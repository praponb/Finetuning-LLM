SELECT id, order_date FROM orders WHERE order_date = (SELECT MIN(order_date) FROM orders)
