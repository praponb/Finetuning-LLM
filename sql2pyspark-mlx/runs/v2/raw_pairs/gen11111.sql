WITH top_web_events AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY session_id ORDER BY event_time, id) AS row_num
    FROM web_events
)
SELECT id, session_id, event_time
FROM top_web_events
WHERE row_num = 1;
