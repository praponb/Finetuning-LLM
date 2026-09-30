SELECT
    session_id,
    MAX(duration_sec) AS max_duration_sec,
    ROUND(AVG(duration_sec), 2) AS avg_duration_sec,
    COUNT(*) AS row_count
FROM web_events
GROUP BY session_id
ORDER BY max_duration_sec DESC
