SELECT QUARTER(customers.signup_date) AS signup_quarter, COUNT(CASE WHEN web_events.page = '/cart' THEN 1 END) AS cart_count, AVG(web_events.duration_sec) AS avg_duration_sec
FROM web_events
JOIN sessions ON web_events.session_id = sessions.id
JOIN customers ON sessions.customer_id = customers.id
WHERE sessions.customer_id = 26
GROUP BY QUARTER(customers.signup_date)
ORDER BY avg_duration_sec DESC;
