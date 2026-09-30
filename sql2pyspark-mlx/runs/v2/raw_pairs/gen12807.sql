WITH top_web_events AS (
    SELECT *, RANK() OVER (PARTITION BY page ORDER BY event_time) AS row_num
    FROM web_events
)
SELECT
    id,
    page,
    event_time,
    duration_sec
FROM top_web_events
WHERE row_num = 1;
