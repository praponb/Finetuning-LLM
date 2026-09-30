SELECT order_date, 'order' AS source FROM orders UNION ALL SELECT start_date, 'subscription' AS source FROM subscriptions;
