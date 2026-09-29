SELECT COUNT(monthly_fee) AS monthly_fee_count, ROUND(AVG(monthly_fee), 2) AS avg_monthly_fee, MAX(start_date) AS latest_start_date
FROM subscriptions
WHERE monthly_fee <= 170 AND start_date BETWEEN '2023-09-01' AND '2023-09-18';
