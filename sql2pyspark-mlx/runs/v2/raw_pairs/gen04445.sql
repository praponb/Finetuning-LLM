SELECT
    id,
    page,
    event_time,
    COALESCE(duration_sec, 0) AS duration_sec_filled
FROM web_events
ORDER BY event_time, id DESC
LIMIT 50
