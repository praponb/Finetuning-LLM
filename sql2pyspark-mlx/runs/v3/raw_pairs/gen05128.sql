SELECT we.event_time, s.country, s.started_at
FROM web_events we
INNER JOIN sessions s ON we.session_id = s.id
WHERE we.event_type NOT IN ('click', 'add_to_cart', 'search') AND we.duration_sec BETWEEN 407 AND 451;
