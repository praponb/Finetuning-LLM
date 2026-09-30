SELECT customers.city, COUNT(*) AS session_count, COUNT(CASE WHEN sessions.device = 'mobile' THEN 1 END) AS mobile_count
FROM sessions
LEFT JOIN customers ON sessions.customer_id = customers.id
WHERE sessions.country NOT IN ('SG', 'JP', 'US')
GROUP BY customers.city
