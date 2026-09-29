SELECT plan, end_date FROM subscriptions WHERE customer_id IN (4, 31) UNION ALL SELECT plan, end_date FROM subscriptions WHERE customer_id = 9;
