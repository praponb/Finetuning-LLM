SELECT id, order_date, customer_id FROM orders WHERE order_date >= (SELECT MAX(order_date) FROM orders);
