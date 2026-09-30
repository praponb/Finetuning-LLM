SELECT CASE WHEN c.age >= 73 THEN 'high' WHEN c.age >= 41 THEN 'mid' ELSE 'low' END AS age_bucket, MAX(s.started_at) AS latest_started_at
FROM sessions s
LEFT OUTER JOIN customers c ON s.customer_id = c.id
GROUP BY CASE WHEN c.age >= 73 THEN 'high' WHEN c.age >= 41 THEN 'mid' ELSE 'low' END
ORDER BY age_bucket DESC;
