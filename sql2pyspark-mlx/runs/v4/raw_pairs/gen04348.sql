SELECT end_date, monthly_fee, status, LENGTH(status) AS status_length FROM subscriptions WHERE plan IN ('standard', 'enterprise') AND plan IN ('basic', 'enterprise') ORDER BY status
