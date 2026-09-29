SELECT id, order_date, status
FROM orders
WHERE order_date = (SELECT MIN(order_date) FROM orders)
ORDER BY status, id DESC
