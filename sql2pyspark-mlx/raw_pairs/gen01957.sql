SELECT DATE_TRUNC('MONTH', end_date) AS end_month, SUM(CASE WHEN status = 'ACTIVE' THEN 1 ELSE 0 END) AS active_count FROM subscriptions WHERE MONTH(start_date) IN (3, 7, 9) GROUP BY end_month
