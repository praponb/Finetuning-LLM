SELECT *, DATEDIFF(start_date, end_date) AS days_end_date_to_start_date FROM subscriptions WHERE MONTH(start_date) IN (2, 8, 12)
