select id, monthly_fee, customer_id, end_date, last_day(end_date) as end_date_month_end from subscriptions where year(start_date) >= 2024 or month(start_date) in (3, 4, 11)
