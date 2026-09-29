SELECT plan, COUNT(CASE WHEN plan = 'premium' THEN 1 END) premium_count
FROM subscriptions
WHERE status = 'CANCELLED' AND plan = 'standard'
GROUP BY plan
