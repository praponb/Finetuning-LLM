SELECT MAX(end_date) AS latest_end_date, MIN(monthly_fee) AS min_monthly_fee
FROM subscriptions
WHERE end_date BETWEEN '2024-08-09' AND '2025-03-01'
ORDER BY latest_end_date DESC;
