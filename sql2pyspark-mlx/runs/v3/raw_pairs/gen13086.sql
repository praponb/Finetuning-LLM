SELECT status, COUNT(monthly_fee) monthly_fee_count
FROM subscriptions
WHERE end_date >= '2025-04-17'
GROUP BY 1
ORDER BY status DESC
LIMIT 50
