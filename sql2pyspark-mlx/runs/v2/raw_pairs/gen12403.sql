SELECT id, order_date FROM orders WHERE order_date = (SELECT MIN(order_date) FROM orders WHERE amount IS NULL) ORDER BY id
