SELECT status, plan, COUNT(monthly_fee) AS monthly_fee_count
FROM subscriptions
WHERE end_date BETWEEN '2024-06-01' AND '2025-06-01' AND monthly_fee BETWEEN 80 AND 100
GROUP BY status, plan
ORDER BY monthly_fee_count
