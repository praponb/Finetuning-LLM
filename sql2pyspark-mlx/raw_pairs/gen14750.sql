select id, end_date, status, round(monthly_fee * 1.07, 2) as adjusted_monthly_fee, cast(monthly_fee as int) as monthly_fee_int from subscriptions where customer_id = 39
