SELECT YEAR(customers.signup_date) AS signup_year, COUNT(DISTINCT web_events.duration_sec) AS unique_sessions
FROM web_events
RIGHT OUTER JOIN sessions ON web_events.session_id = sessions.id
FULL OUTER JOIN customers ON sessions.customer_id = customers.id
WHERE customers.city IN ('Singapore', 'New York', 'Chiang Mai')
GROUP BY YEAR(customers.signup_date)
ORDER BY unique_sessions ASC;
