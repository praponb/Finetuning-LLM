SELECT
    page,
    SUM(CASE WHEN event_type = 'purchase' THEN 1 ELSE 0 END) AS purchase_count,
    MIN(duration_sec) AS min_duration_sec,
    MAX(duration_sec) AS max_duration_sec
FROM web_events
WHERE session_id IN (32, 36)
GROUP BY page
