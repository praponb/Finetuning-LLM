SELECT
    id,
    event_type,
    page,
    event_time
FROM web_events
ORDER BY page ASC, 1 DESC
