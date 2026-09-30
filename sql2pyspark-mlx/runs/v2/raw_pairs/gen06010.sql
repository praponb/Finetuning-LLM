SELECT
    s.id,
    s.started_at,
    s.browser,
    t.search_count
FROM sessions s
INNER JOIN (SELECT session_id, SUM(CASE WHEN page = '/search' THEN 1 ELSE 0 END) AS search_count
    FROM web_events
    WHERE duration_sec BETWEEN 171 AND 430
    GROUP BY session_id) t ON s.id = t.session_id;
