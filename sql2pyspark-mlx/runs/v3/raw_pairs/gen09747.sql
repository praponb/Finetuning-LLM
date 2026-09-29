WITH SessionSummary AS (
    SELECT session_id, ROUND(STDDEV(duration_sec), 2) AS stddev_duration_sec, MAX(duration_sec) AS max_duration_sec
    FROM web_events
    GROUP BY session_id
)
SELECT
    sessions.customer_id,
    sessions.country,
    SessionSummary.stddev_duration_sec,
    SessionSummary.max_duration_sec
FROM sessions
INNER JOIN SessionSummary ON sessions.id = SessionSummary.session_id
