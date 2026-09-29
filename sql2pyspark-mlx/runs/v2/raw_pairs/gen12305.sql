SELECT we.event_type, s.browser
FROM web_events we
RIGHT JOIN sessions s ON we.session_id = s.id
WHERE s.browser IN ('Firefox', 'Chrome') AND s.country IN ('DE', 'US', 'SG');
