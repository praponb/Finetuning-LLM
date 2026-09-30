SELECT order_date FROM orders WHERE order_date IS NOT NULL INTERSECT SELECT end_date FROM subscriptions WHERE end_date IS NOT NULL;
