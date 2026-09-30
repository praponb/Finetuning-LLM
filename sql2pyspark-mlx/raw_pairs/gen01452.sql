SELECT
    s.*,
    top1.web_event_duration_sec,
    top1.event_type
FROM sessions s
JOIN LATERAL (
        SELECT c.duration_sec AS web_event_duration_sec, c.event_type
        FROM web_events c
        WHERE c.session_id = s.id
        ORDER BY c.duration_sec DESC, c.id DESC
        LIMIT 1
    ) top1 ON TRUE;
