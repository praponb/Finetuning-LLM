SELECT status, COUNT(CASE WHEN plan = 'enterprise' THEN 1 END) AS enterprise_count
FROM subscriptions
GROUP BY 1
HAVING enterprise_count < 2
ORDER BY enterprise_count DESC, status;
