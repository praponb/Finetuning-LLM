SELECT status, SUM(monthly_fee) AS total_monthly_fee, MAX(monthly_fee) AS max_monthly_fee
FROM subscriptions
WHERE customer_id IS NOT NULL AND status <> 'ACTIVE'
GROUP BY status
