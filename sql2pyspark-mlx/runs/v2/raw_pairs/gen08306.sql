SELECT *
FROM (
    SELECT HOUR(event_time) AS event_hour, session_id, ROUND(AVG(duration_sec), 2) AS avg_duration_sec
    FROM web_events
    GROUP BY HOUR(event_time), session_id
) AS s
WHERE avg_duration_sec < 37
ORDER BY session_id ASC;
