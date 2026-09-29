SELECT CASE WHEN customers.country IN ('US', 'DE') THEN 'tier_1' ELSE 'other' END AS country_group, MAX(sessions.started_at) AS last_started_at
FROM sessions
LEFT JOIN customers ON sessions.customer_id = customers.id
WHERE customers.email LIKE '%@outlook.com'
GROUP BY CASE WHEN customers.country IN ('US', 'DE') THEN 'tier_1' ELSE 'other' END
ORDER BY country_group DESC;
