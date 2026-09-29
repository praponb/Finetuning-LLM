select id, end_date, start_date, status, cast(monthly_fee as int) monthly_fee_int, upper(plan) plan_upper from subscriptions where monthly_fee >= 90 or status = 'ACTIVE'
