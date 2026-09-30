SELECT
    web_events.event_time,
    sessions.browser,
    customers.city,
    customers.signup_date
FROM web_events
JOIN sessions ON web_events.session_id = sessions.id
FULL OUTER JOIN customers ON sessions.customer_id = customers.id
ORDER BY sessions.browser;
