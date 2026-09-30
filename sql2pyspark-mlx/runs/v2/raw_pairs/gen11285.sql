WITH top_web_events AS (
    SELECT
        id,
        session_id,
        duration_sec,
        event_type,
        DENSE_RANK() OVER (PARTITION BY session_id ORDER BY duration_sec DESC) AS rn
    FROM web_events
)
SELECT *
FROM top_web_events
WHERE rn = 1;
