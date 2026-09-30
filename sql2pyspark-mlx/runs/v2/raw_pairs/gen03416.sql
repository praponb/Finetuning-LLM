WITH ranked_web_events AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY session_id ORDER BY duration_sec DESC, id) AS pos
    FROM web_events
    WHERE event_time >= '2023-08-17'
)
SELECT *
FROM ranked_web_events
WHERE pos <= 3;
