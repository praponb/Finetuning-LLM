SELECT id, plan, status, DATEDIFF(end_date, start_date) AS days_start_date_to_end_date FROM subscriptions WHERE monthly_fee > 70;
