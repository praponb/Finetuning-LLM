SELECT id, customer_id, start_date, plan, DATEDIFF(start_date, end_date) AS days_end_date_to_start_date, DATE_SUB(end_date, 69) AS end_date_minus_69d FROM subscriptions
