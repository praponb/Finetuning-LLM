WITH session_totals AS (
    SELECT session_id, SORT_ARRAY(COLLECT_SET(page)) AS page_list, AVG(duration_sec) AS avg_duration_sec
    FROM web_events
    WHERE event_time >= '2024-04-29'
    GROUP BY session_id
)
SELECT
    s.device,
    s.started_at,
    st.page_list,
    st.avg_duration_sec
FROM sessions s
INNER JOIN session_totals st ON s.id = st.session_id
ORDER BY s.started_at, avg_duration_sec
