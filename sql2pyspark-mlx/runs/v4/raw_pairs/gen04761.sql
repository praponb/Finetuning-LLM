SELECT start_date, 'subscription' AS source FROM subscriptions WHERE YEAR(start_date) >= 2023 UNION ALL SELECT order_date, 'order' AS source FROM orders WHERE status IN ('SHIPPED', 'PENDING')
