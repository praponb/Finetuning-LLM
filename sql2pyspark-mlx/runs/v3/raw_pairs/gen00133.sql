SELECT
    session_id,
    page,
    COUNT(duration_sec) AS duration_sec_count,
    MIN(duration_sec) AS min_duration_sec
FROM web_events
WHERE duration_sec >= 176 AND duration_sec <= 345
GROUP BY session_id, page
HAVING MIN(duration_sec) >= 340;
