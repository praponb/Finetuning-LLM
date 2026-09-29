SELECT customer_id, SUM(CASE WHEN status = 'EXPIRED' THEN monthly_fee ELSE 0 END) AS expired_monthly_fee, MAX(monthly_fee) - MIN(monthly_fee) AS monthly_fee_range
FROM subscriptions
WHERE plan IN ('premium', 'enterprise')
GROUP BY customer_id
HAVING SUM(CASE WHEN status = 'EXPIRED' THEN monthly_fee ELSE 0 END) > 24
ORDER BY expired_monthly_fee
