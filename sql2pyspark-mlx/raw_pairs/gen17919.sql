WITH ranked_web_events AS (
    SELECT
        id,
        page,
        duration_sec,
        session_id,
        ROW_NUMBER() OVER (PARTITION BY page ORDER BY duration_sec, id) AS rn
    FROM web_events
    WHERE page IN ('/search', '/checkout', '/product')
)
SELECT *
FROM ranked_web_events
WHERE rn <= 3
