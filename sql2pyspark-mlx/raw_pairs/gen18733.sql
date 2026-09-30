SELECT id, status, monthly_fee, end_date, SUBSTRING(plan, 1, 2) AS plan_prefix FROM subscriptions ORDER BY end_date, id LIMIT 3;
