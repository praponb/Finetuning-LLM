SELECT CASE WHEN sessions.country IN ('UK', 'US') THEN 'tier_1' ELSE 'other' END AS country_group, COUNT(web_events.duration_sec) AS duration_sec_count
FROM web_events
RIGHT JOIN sessions ON sessions.id = web_events.session_id
LEFT JOIN customers ON customers.id = sessions.customer_id
WHERE web_events.session_id = 30
GROUP BY CASE WHEN sessions.country IN ('UK', 'US') THEN 'tier_1' ELSE 'other' END
