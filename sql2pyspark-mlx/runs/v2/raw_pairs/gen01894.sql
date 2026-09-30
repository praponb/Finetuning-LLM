SELECT id, monthly_fee, start_date, plan, CAST(monthly_fee AS INT) monthly_fee_int FROM subscriptions ORDER BY plan
