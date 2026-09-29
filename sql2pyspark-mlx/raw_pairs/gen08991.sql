WITH with_latest AS (
    SELECT
        s.id,
        s.started_at,
        x.web_event_page
    FROM sessions s
    LEFT JOIN LATERAL (
            SELECT c.page AS web_event_page
            FROM web_events c
            WHERE c.session_id = s.id
              AND c.event_time <= s.started_at
            ORDER BY c.event_time DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
)
SELECT id, web_event_page
FROM with_latest;
