SELECT status FROM orders EXCEPT SELECT status FROM subscriptions WHERE customer_id IS NULL;
