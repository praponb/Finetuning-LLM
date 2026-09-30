SELECT DATE_TRUNC('MONTH', start_date) AS start_month, COUNT(CASE WHEN plan = 'enterprise' THEN 1 END) AS enterprise_count
FROM subscriptions
GROUP BY DATE_TRUNC('MONTH', start_date);
