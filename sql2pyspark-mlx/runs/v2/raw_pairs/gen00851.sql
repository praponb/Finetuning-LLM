select start_date, monthly_fee, plan from subscriptions where month(end_date) in (4, 6, 7) and customer_id is not null
