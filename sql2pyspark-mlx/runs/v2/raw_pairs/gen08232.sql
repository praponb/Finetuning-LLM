SELECT id, plan, end_date, monthly_fee, SUM(monthly_fee) OVER (PARTITION BY plan ORDER BY end_date, id) AS running_monthly_fee FROM subscriptions WHERE monthly_fee < 110 ORDER BY end_date;
