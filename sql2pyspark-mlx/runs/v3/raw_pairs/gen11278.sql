SELECT customers.country, DATE_TRUNC('MONTH', customers.signup_date) AS signup_month, AVG(web_events.duration_sec) AS avg_duration_sec
FROM web_events
INNER JOIN sessions ON sessions.id = web_events.session_id
RIGHT JOIN customers ON customers.id = sessions.customer_id
GROUP BY customers.country, DATE_TRUNC('MONTH', customers.signup_date)
