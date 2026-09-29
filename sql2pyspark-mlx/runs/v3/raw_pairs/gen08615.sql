SELECT DATE_FORMAT(start_date, 'yyyy-MM') AS start_month, SUM(monthly_fee) AS total_monthly_fee, DENSE_RANK() OVER (ORDER BY SUM(monthly_fee) DESC) AS monthly_fee_rank
FROM subscriptions
WHERE start_date > '2025-09-01'
GROUP BY DATE_FORMAT(start_date, 'yyyy-MM')
ORDER BY monthly_fee_rank
