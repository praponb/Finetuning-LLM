SELECT id, end_date, monthly_fee, SUBSTRING(status, 1, 4) AS status_prefix FROM subscriptions WHERE start_date < '2024-02-01' ORDER BY end_date, 3 DESC
