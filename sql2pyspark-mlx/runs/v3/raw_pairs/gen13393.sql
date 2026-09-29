SELECT YEAR(start_date) AS start_year, ROUND(AVG(monthly_fee), 2) AS avg_monthly_fee, COUNT(monthly_fee) AS monthly_fee_count
FROM subscriptions
GROUP BY start_year;
