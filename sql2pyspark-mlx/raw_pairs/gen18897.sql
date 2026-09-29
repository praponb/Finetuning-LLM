WITH top_web_events AS (
    SELECT *, RANK() OVER (PARTITION BY page ORDER BY event_time DESC) AS rn
    FROM web_events
)
SELECT *
FROM top_web_events
WHERE rn = 1
ORDER BY event_time DESC
