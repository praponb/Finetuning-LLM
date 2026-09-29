SELECT id, status, monthly_fee, MAX(monthly_fee) OVER (PARTITION BY status) status_max_monthly_fee FROM subscriptions;
