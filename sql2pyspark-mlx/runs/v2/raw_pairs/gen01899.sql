SELECT plan, ROUND((100 * SUM(CASE WHEN plan = 'basic' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_basic, COUNT(DISTINCT monthly_fee) AS unique_plans
FROM subscriptions
WHERE status IN ('CANCELLED', 'ACTIVE', 'PAUSED') AND (status = 'EXPIRED' OR customer_id = 35)
GROUP BY plan
