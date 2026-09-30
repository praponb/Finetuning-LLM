SELECT customer_id, status, SUM(CASE WHEN plan = 'standard' THEN 1 ELSE 0 END) AS standard_count
FROM subscriptions
WHERE monthly_fee > 60 AND status IN ('ACTIVE', 'CANCELLED', 'PAUSED')
GROUP BY customer_id, status
HAVING standard_count > 20
