SELECT id, plan, status, ROUND(monthly_fee, 1) monthly_fee_rounded FROM subscriptions WHERE plan IN ('enterprise', 'premium', 'standard') AND start_date >= '2023-10-13' AND YEAR(start_date) = 2024
