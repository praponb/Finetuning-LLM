SELECT status, MIN(order_date) AS min_order_date FROM orders GROUP BY status
