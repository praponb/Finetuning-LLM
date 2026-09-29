SELECT DATE_FORMAT(start_date, 'yyyy-MM') AS start_month, plan, MIN(end_date) AS first_end_date
FROM subscriptions
WHERE plan = 'enterprise'
GROUP BY DATE_FORMAT(start_date, 'yyyy-MM'), plan
ORDER BY 3;
