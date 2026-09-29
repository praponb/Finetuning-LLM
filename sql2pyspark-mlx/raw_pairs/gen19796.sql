SELECT
    s.customer_id,
    QUARTER(s.started_at) AS started_quarter,
    MIN(we.event_time) AS min_event_time,
    MAX(we.event_time) AS latest_event_time
FROM web_events we
LEFT JOIN sessions s ON we.session_id = s.id
GROUP BY s.customer_id, QUARTER(s.started_at)
ORDER BY min_event_time ASC, customer_id ASC, started_quarter ASC
LIMIT 10
