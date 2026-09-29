SELECT CASE WHEN plan IN ('standard', 'enterprise') THEN 'group_a' ELSE 'other' END AS plan_group, status, COUNT(*) AS subscription_count
FROM subscriptions
WHERE plan = 'standard' OR status NOT IN ('ACTIVE', 'CANCELLED')
GROUP BY 1, 2
HAVING COUNT(*) > 2
ORDER BY status, plan_group
LIMIT 10;
