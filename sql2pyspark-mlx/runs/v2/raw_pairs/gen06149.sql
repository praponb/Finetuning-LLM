SELECT id, status, monthly_fee, MIN(monthly_fee) OVER (PARTITION BY status) AS status_min_monthly_fee FROM subscriptions
