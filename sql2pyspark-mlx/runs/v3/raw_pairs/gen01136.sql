WITH ranked_web_events AS (
    SELECT
        id,
        page,
        duration_sec,
        session_id,
        ROW_NUMBER() OVER (PARTITION BY page ORDER BY duration_sec DESC, id) AS pos
    FROM web_events
)
SELECT
    id,
    page,
    duration_sec,
    session_id
FROM ranked_web_events
WHERE pos <= 2
ORDER BY 2, duration_sec DESC
