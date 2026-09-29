SELECT
    web_events.duration_sec,
    sessions.browser,
    customers.age,
    customers.credit_limit
FROM web_events
LEFT JOIN sessions ON web_events.session_id = sessions.id
INNER JOIN customers ON sessions.customer_id = customers.id
WHERE web_events.event_type IN ('click', 'purchase', 'add_to_cart')
ORDER BY browser;
