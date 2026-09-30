WITH filtered AS (
    SELECT
        id,
        browser,
        country,
        CASE WHEN device = 'tablet' THEN 1 ELSE 0 END is_tablet,
        DATEDIFF(CURRENT_DATE(), started_at) days_since_started_at
    FROM sessions
    WHERE started_at > TIMESTAMP '2023-03-23 02:00:00'
      AND country IS NOT NULL
)
SELECT
    s.id,
    s.is_tablet,
    x.page,
    x.event_type
FROM filtered s
JOIN LATERAL (
        SELECT c.page, c.event_type
        FROM web_events c
        WHERE c.session_id = s.id
        ORDER BY c.duration_sec DESC, c.id DESC
        LIMIT 1
    ) x ON TRUE
