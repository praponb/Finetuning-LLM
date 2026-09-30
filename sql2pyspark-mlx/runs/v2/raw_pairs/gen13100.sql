WITH Base AS (
    SELECT *
    FROM web_events
    WHERE duration_sec <> 124
),
SessionIdSummary AS (
    SELECT session_id, SUM(duration_sec) AS total_duration_sec, ROUND(AVG(duration_sec), 2) AS avg_duration_sec
    FROM Base
    GROUP BY session_id
)
SELECT *
FROM SessionIdSummary
WHERE total_duration_sec > 1100;
