SELECT status, SUM(CASE WHEN plan = 'basic' THEN 1 ELSE 0 END) AS basic_count
FROM subscriptions
WHERE plan = 'standard' AND (status NOT IN ('CANCELLED', 'ACTIVE') OR monthly_fee >= 70)
GROUP BY 1
HAVING COUNT(monthly_fee) > 5
