SELECT orders.id, orders.status, orders.order_date
FROM orders
WHERE YEAR(orders.order_date) = 2023 AND orders.id IN (SELECT order_id FROM payments WHERE status != 'SUCCESS');
