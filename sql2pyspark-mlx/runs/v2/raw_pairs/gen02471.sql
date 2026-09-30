SELECT customers.country, MAX(web_events.duration_sec) AS max_duration_sec
FROM web_events
INNER JOIN sessions ON web_events.session_id = sessions.id
LEFT JOIN customers ON sessions.customer_id = customers.id
WHERE sessions.browser IN ('Safari', 'Chrome')
GROUP BY customers.country;
