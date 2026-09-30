SELECT id, start_date, monthly_fee, plan FROM subscriptions WHERE start_date = (SELECT MAX(start_date) FROM subscriptions);
