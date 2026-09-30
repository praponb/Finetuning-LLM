SELECT id, start_date, end_date, monthly_fee, DATEDIFF(start_date, end_date) / 7 AS weeks_end_date_to_start_date FROM subscriptions WHERE status = 'EXPIRED'
