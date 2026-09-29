SELECT status, 'order' AS source FROM orders WHERE MONTH(order_date) IN (4, 5, 8) UNION SELECT status, 'subscription' AS source FROM subscriptions WHERE end_date <= '2024-11-01'
