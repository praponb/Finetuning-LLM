SELECT status, ROUND(AVG(monthly_fee), 2) AS avg_monthly_fee, SUM(monthly_fee) AS total_monthly_fee FROM subscriptions GROUP BY 1 HAVING SUM(monthly_fee) > 410 ORDER BY status LIMIT 10;
