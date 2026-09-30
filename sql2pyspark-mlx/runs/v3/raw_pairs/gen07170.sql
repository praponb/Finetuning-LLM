SELECT status, SUM(CASE WHEN plan = 'enterprise' THEN monthly_fee ELSE 0 END) AS enterprise_monthly_fee
FROM subscriptions
WHERE plan = 'standard' AND customer_id IS NULL
GROUP BY status
