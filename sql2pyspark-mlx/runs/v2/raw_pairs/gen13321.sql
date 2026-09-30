SELECT
    YEAR(customers.signup_date) AS signup_year,
    DATE_TRUNC('MONTH', customers.signup_date) AS signup_month,
    MAX(web_events.duration_sec) - MIN(web_events.duration_sec) AS duration_sec_range,
    COUNT(DISTINCT web_events.duration_sec) AS unique_sessions
FROM web_events
LEFT OUTER JOIN sessions ON web_events.session_id = sessions.id
INNER JOIN customers ON sessions.customer_id = customers.id
WHERE web_events.event_type != 'click'
GROUP BY YEAR(customers.signup_date), DATE_TRUNC('MONTH', customers.signup_date)
HAVING COUNT(DISTINCT web_events.duration_sec) > 4
ORDER BY signup_month;
