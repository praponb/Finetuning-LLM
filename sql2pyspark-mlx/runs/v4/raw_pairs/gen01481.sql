WITH recent_web_events AS (
    SELECT *
    FROM web_events
    WHERE event_type = 'purchase' AND duration_sec < 405 AND event_time < '2024-12-24'
),
Agg AS (
    SELECT event_type, COUNT(*) AS cnt
    FROM recent_web_events
    GROUP BY event_type
)
SELECT event_type, cnt
FROM Agg
WHERE cnt >= 6
ORDER BY event_type
