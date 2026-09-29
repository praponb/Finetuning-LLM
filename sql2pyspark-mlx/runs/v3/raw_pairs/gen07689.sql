SELECT COUNT(CASE WHEN plan = 'basic' THEN 1 END) AS basic_count
FROM subscriptions
ORDER BY basic_count;
