SELECT id, order_date, store_id
FROM orders
WHERE order_date = (SELECT MIN(order_date) FROM orders)
ORDER BY store_id;
