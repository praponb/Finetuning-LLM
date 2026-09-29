WITH valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        TO_DATE(started_at) AS started_at_date,
        COALESCE(country, 'NONE') AS resolved_country
    FROM sessions
    WHERE started_at < '2023-10-05'
)
-- Latest matching web_event per row (LATERAL ... LIMIT 1)
SELECT
    s.*,
    x.web_event_duration_sec,
    x.web_event_event_time
FROM valid s
JOIN LATERAL (
        SELECT c.duration_sec AS web_event_duration_sec, c.event_time AS web_event_event_time
        FROM web_events c
        WHERE c.session_id = s.id
        ORDER BY c.duration_sec DESC, c.id DESC
        LIMIT 1
    ) x ON TRUE
