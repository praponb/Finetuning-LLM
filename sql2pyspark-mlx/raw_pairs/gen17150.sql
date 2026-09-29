SELECT customer_id, id FROM subscriptions EXCEPT SELECT customer_id, id FROM subscriptions WHERE plan IN ('standard', 'basic', 'premium')
