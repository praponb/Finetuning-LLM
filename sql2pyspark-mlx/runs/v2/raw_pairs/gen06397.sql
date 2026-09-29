SELECT status, MAX(order_date) AS last_order_date FROM orders GROUP BY 1;
