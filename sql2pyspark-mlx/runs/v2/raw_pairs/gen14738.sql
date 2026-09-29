SELECT id, monthly_fee, TRIM(status) AS clean_status FROM subscriptions WHERE customer_id IS NOT NULL ORDER BY clean_status DESC;
