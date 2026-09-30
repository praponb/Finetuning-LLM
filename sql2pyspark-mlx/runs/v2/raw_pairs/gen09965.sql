SELECT plan, COUNT(CASE WHEN plan = 'enterprise' THEN 1 END) enterprise_count, COUNT(*) total_count
FROM subscriptions
GROUP BY plan
ORDER BY enterprise_count DESC, total_count DESC, plan ASC
LIMIT 5;
