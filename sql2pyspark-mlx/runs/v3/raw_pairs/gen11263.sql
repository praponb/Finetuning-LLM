SELECT
    id,
    page,
    duration_sec,
    event_time
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY page ORDER BY duration_sec DESC, id) row_num
    FROM web_events
    WHERE session_id IN (7, 26)
) ranked
WHERE row_num <= 2
