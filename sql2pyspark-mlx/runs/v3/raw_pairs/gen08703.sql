SELECT monthly_fee, end_date, TRIM(plan) AS clean_plan FROM subscriptions ORDER BY end_date;
