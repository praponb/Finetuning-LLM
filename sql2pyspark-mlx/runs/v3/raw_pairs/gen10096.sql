SELECT start_date FROM subscriptions INTERSECT SELECT order_date FROM orders ORDER BY start_date;
