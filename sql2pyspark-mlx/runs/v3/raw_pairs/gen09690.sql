SELECT id, customer_id FROM subscriptions WHERE customer_id IS NOT NULL INTERSECT SELECT id, customer_id FROM subscriptions WHERE customer_id IN (10, 17, 18);
