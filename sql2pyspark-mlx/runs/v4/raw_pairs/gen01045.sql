WITH filtered AS (
    SELECT
        *,
        LOWER(country) AS country_lower,
        LOWER(browser) AS browser_lower
    FROM sessions
    WHERE started_at > TO_TIMESTAMP('2024-04-04 03:00:00')
)
SELECT s.*, top1.duration_sec
FROM filtered s
LEFT JOIN LATERAL (
        SELECT c.duration_sec
        FROM web_events c
        WHERE c.session_id = s.id
          AND c.event_time <= s.started_at
        ORDER BY c.event_time DESC, c.id DESC
        LIMIT 1
    ) top1 ON TRUE
