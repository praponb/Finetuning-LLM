SELECT status, MIN(monthly_fee) AS min_monthly_fee
FROM subscriptions
WHERE start_date BETWEEN '2024-01-17' AND '2024-07-08'
GROUP BY status
HAVING MIN(monthly_fee) > 200
ORDER BY 1 DESC
