SELECT id, end_date, monthly_fee, status FROM subscriptions WHERE end_date <= (SELECT MIN(end_date) FROM subscriptions)
