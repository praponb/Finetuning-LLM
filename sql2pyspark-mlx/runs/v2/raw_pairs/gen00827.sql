SELECT id, order_date FROM orders WHERE order_date = (SELECT MIN(order_date) FROM orders WHERE discount >= 0.19)
