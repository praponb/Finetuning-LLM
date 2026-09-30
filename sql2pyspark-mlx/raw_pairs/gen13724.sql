SELECT
    id,
    session_id,
    event_type,
    page,
    duration_sec
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY session_id ORDER BY event_time DESC, id) AS row_num
    FROM web_events
) ranked
WHERE row_num = 1;
