SELECT shipped_date FROM shipments INTERSECT SELECT start_date FROM subscriptions WHERE customer_id IN (19, 23, 24);
