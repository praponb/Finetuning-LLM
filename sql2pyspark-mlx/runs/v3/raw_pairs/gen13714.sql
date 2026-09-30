SELECT id, start_date, LAST_DAY(start_date) AS start_date_month_end, LOWER(plan) AS plan_lower FROM subscriptions WHERE plan IN ('enterprise', 'premium', 'basic')
