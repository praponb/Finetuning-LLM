SELECT id, order_date, shipping_fee
FROM orders
WHERE order_date = (SELECT MIN(order_date) FROM orders);
