WITH web_events_by_session AS (
    SELECT session_id, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(page))) AS page_values, COUNT(*) AS web_event_count
    FROM web_events
    WHERE page IN ('/cart', '/search')
    GROUP BY session_id
)
SELECT s.device, webs.page_values, webs.web_event_count
FROM sessions s
JOIN web_events_by_session webs ON s.id = webs.session_id;
