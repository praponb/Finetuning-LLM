SELECT id, customer_id, status, ROUND(monthly_fee * 1.1, 2) adjusted_monthly_fee FROM subscriptions WHERE plan IN ('enterprise', 'premium')
