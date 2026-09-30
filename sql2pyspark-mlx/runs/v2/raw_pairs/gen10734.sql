WITH session_summary AS (
    SELECT session_id, MAX(duration_sec) - MIN(duration_sec) AS duration_sec_range
    FROM web_events
    WHERE duration_sec > 183
    GROUP BY session_id
)
SELECT s.customer_id, s.country, ss.duration_sec_range
FROM sessions s
INNER JOIN session_summary ss ON s.id = ss.session_id
WHERE ss.duration_sec_range >= 79
ORDER BY country;
