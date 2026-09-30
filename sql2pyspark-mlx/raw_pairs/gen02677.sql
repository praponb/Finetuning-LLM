SELECT end_date, CAST(monthly_fee AS INT) monthly_fee_int, TRIM(plan) clean_plan
FROM subscriptions
