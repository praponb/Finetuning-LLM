SELECT monthly_fee, status, DATE_TRUNC('month', end_date) end_date_period, ROUND(monthly_fee * 1.15, 2) monthly_fee_adjusted FROM subscriptions ORDER BY status
