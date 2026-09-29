SELECT status, MAX(monthly_fee) AS max_monthly_fee, COUNT(CASE WHEN plan = 'enterprise' THEN 1 END) AS enterprise_count
FROM subscriptions
GROUP BY status
HAVING COUNT(CASE WHEN plan = 'enterprise' THEN 1 END) >= 4
ORDER BY status
