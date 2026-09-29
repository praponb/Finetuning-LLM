SELECT s.device, t.page_view_count
FROM sessions s
INNER JOIN (SELECT session_id, SUM(CASE WHEN event_type = 'page_view' THEN 1 ELSE 0 END) AS page_view_count
    FROM web_events
    GROUP BY session_id) t ON s.id = t.session_id
WHERE t.page_view_count > 28
ORDER BY device DESC, page_view_count DESC;
