WITH with_last AS (
    -- Latest matching web_event per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.customer_id,
        s.device,
        s.started_at,
        latest.web_event_page,
        latest.duration_sec
    FROM sessions s
    LEFT JOIN LATERAL (
            SELECT c.page AS web_event_page, c.duration_sec
            FROM web_events c
            WHERE c.session_id = s.id
            ORDER BY c.duration_sec DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
)
SELECT
    id,
    customer_id,
    device,
    web_event_page
FROM with_last
ORDER BY customer_id
