SELECT id, end_date, start_date, DATEDIFF(CURRENT_DATE(), end_date) AS days_since_end_date FROM subscriptions WHERE MONTH(start_date) IN (5, 7, 12);
