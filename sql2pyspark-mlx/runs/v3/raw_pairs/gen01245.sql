SELECT id, plan, status, DATEDIFF(CURRENT_DATE(), end_date) AS days_since_end_date FROM subscriptions WHERE customer_id IS NOT NULL ORDER BY status, id LIMIT 3;
