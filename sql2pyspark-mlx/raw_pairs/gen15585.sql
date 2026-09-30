SELECT id, start_date, end_date, monthly_fee FROM subscriptions WHERE start_date = (SELECT MAX(start_date) FROM subscriptions);
