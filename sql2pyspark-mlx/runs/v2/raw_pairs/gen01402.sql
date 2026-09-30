select id, start_date, plan, monthly_fee, round(monthly_fee, 0) as monthly_fee_rounded, concat_ws('/', plan, status) as plan_status_key from subscriptions where monthly_fee <= 70 and plan = 'basic'
