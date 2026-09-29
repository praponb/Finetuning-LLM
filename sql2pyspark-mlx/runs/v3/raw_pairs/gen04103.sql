SELECT MONTH(c.signup_date) AS signup_month, MIN(we.event_time) AS first_event_time
FROM web_events we
INNER JOIN sessions s ON we.session_id = s.id
LEFT OUTER JOIN customers c ON s.customer_id = c.id
WHERE we.event_type IN ('search', 'purchase', 'click')
GROUP BY MONTH(c.signup_date)
