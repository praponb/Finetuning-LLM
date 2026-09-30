SELECT sessions.browser, SUM(CASE WHEN web_events.page = '/search' THEN web_events.duration_sec ELSE 0 END) AS search_duration_sec
FROM web_events
JOIN sessions ON web_events.session_id = sessions.id
JOIN customers ON sessions.customer_id = customers.id
WHERE customers.age != 65
GROUP BY sessions.browser;
