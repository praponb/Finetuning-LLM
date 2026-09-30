SELECT we.event_type, s.customer_id, c.country
FROM web_events we
RIGHT OUTER JOIN sessions s ON we.session_id = s.id
RIGHT OUTER JOIN customers c ON s.customer_id = c.id
WHERE s.country = 'SG';
