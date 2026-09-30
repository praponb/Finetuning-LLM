SELECT status, MAX(monthly_fee) AS max_monthly_fee, SUM(CASE WHEN plan = 'basic' THEN 1 ELSE 0 END) AS basic_count
FROM subscriptions
WHERE monthly_fee BETWEEN 130 AND 180 AND customer_id = 32
GROUP BY status
