SELECT customer_id, end_date FROM subscriptions WHERE status = 'PAUSED' UNION SELECT customer_id, end_date FROM subscriptions WHERE plan = 'premium';
