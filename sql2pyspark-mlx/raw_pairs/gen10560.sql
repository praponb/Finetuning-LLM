WITH with_last AS (
    -- Latest matching web_event per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.device,
        s.country,
        latest.web_event_duration_sec
    FROM sessions s
    LEFT JOIN LATERAL (
            SELECT c.duration_sec AS web_event_duration_sec
            FROM web_events c
            WHERE c.session_id = s.id
            ORDER BY c.event_time DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
)
SELECT
    id,
    device,
    country,
    web_event_duration_sec,
    CASE WHEN device RLIKE '^(mobile|desktop)$' THEN 'GROUP_B' WHEN device RLIKE '^(tablet)$' THEN 'PRIORITY' ELSE 'CORE' END AS device_zone
FROM with_last
ORDER BY device_zone DESC
