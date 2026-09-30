SELECT session_id, page, MAX(duration_sec) AS max_duration_sec
FROM web_events
GROUP BY session_id, page
ORDER BY session_id DESC, max_duration_sec DESC;
