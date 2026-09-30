SELECT customer_id, status FROM subscriptions WHERE status = 'ACTIVE' UNION ALL SELECT customer_id, status FROM subscriptions WHERE plan = 'enterprise' ORDER BY customer_id DESC;
