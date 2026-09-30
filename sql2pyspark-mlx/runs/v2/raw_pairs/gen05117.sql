SELECT MAX(end_date) AS latest_end_date FROM subscriptions WHERE MONTH(start_date) IN (2, 3, 12)
