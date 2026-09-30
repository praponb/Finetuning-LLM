SELECT id, end_date, monthly_fee, LEAD(monthly_fee, 1) OVER (ORDER BY end_date, id) AS next_monthly_fee FROM subscriptions WHERE YEAR(end_date) >= 2024 ORDER BY end_date DESC;
