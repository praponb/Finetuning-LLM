SELECT
    we.event_type,
    s.started_at,
    c.name,
    c.age,
    c.age * c.credit_limit AS age_times_credit_limit
FROM web_events we
INNER JOIN sessions s ON we.session_id = s.id
INNER JOIN customers c ON s.customer_id = c.id
WHERE s.country = 'SG';
