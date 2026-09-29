SELECT status FROM payments INTERSECT SELECT status FROM subscriptions WHERE customer_id IS NULL;
