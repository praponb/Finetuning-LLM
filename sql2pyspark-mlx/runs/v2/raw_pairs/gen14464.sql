SELECT id, end_date FROM subscriptions WHERE customer_id = 34 EXCEPT SELECT id, end_date FROM subscriptions WHERE customer_id IS NULL;
