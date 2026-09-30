SELECT *, CONCAT(status, '/', plan) AS status_plan FROM subscriptions WHERE plan = 'enterprise' ORDER BY end_date;
