WITH session_summary AS (
    SELECT session_id, COUNT(DISTINCT duration_sec) unique_pages, COUNT(duration_sec) duration_sec_count
    FROM web_events
    WHERE duration_sec BETWEEN 268 AND 303
    GROUP BY session_id
)
SELECT
    s.id,
    s.country,
    s.browser,
    ss.unique_pages,
    ss.duration_sec_count
FROM sessions s
LEFT JOIN session_summary ss ON s.id = ss.session_id
WHERE ss.duration_sec_count >= 5
