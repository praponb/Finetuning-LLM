SELECT id, plan, end_date, status, FIRST_VALUE(status) OVER (PARTITION BY plan ORDER BY end_date, id) AS first_status FROM subscriptions
