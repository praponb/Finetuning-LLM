SELECT customer_id, ROUND(AVG(monthly_fee), 2) AS avg_monthly_fee
FROM subscriptions
WHERE status IN ('ACTIVE', 'PAUSED', 'EXPIRED') AND status = 'ACTIVE'
GROUP BY customer_id
