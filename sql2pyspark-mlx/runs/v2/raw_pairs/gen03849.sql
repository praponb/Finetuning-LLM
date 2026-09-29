SELECT status FROM orders WHERE customer_id IN (21, 28) UNION ALL SELECT status FROM subscriptions;
