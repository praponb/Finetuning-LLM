WITH ranked_web_events AS (
    SELECT
        id,
        event_type,
        duration_sec,
        ROW_NUMBER() OVER (PARTITION BY event_type ORDER BY duration_sec, id) AS rnk
    FROM web_events
)
SELECT *
FROM ranked_web_events
WHERE rnk <= 5
ORDER BY id;
