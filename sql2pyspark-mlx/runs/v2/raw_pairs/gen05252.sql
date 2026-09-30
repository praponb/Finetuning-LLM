SELECT CASE WHEN customers.age >= 57 THEN 'high' WHEN customers.age >= 35 THEN 'mid' ELSE 'low' END age_bucket, SORT_ARRAY(COLLECT_SET(sessions.device)) device_list
FROM sessions
LEFT JOIN customers ON customers.id = sessions.customer_id
WHERE sessions.browser IS NOT NULL
GROUP BY CASE WHEN customers.age >= 57 THEN 'high' WHEN customers.age >= 35 THEN 'mid' ELSE 'low' END
ORDER BY age_bucket DESC
