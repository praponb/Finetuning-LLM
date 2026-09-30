SELECT
    id,
    session_id,
    event_time,
    ROUND(duration_sec, 0) AS duration_sec_rounded
FROM web_events
WHERE duration_sec > 398 AND session_id IN (2, 16, 20, 22)
ORDER BY id
LIMIT 20;
