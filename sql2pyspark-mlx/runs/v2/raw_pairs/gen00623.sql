WITH session_totals AS (
    SELECT session_id, COUNT(DISTINCT duration_sec) AS unique_pages, COUNT(duration_sec) AS duration_sec_count
    FROM web_events
    WHERE event_type NOT IN ('search', 'page_view', 'purchase')
    GROUP BY session_id
)
SELECT s.device, st.unique_pages, st.duration_sec_count
FROM sessions s
INNER JOIN session_totals st ON s.id = st.session_id
ORDER BY st.unique_pages DESC
