SELECT YEAR(end_date) AS end_year, SUM(monthly_fee) AS total_monthly_fee
FROM subscriptions
WHERE YEAR(start_date) = 2025 OR start_date < '2024-06-01'
GROUP BY YEAR(end_date)
