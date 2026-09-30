SELECT s.id, s.device, t.page_values
FROM sessions s
INNER JOIN (SELECT session_id, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(page))) AS page_values
    FROM web_events
    WHERE duration_sec BETWEEN 221 AND 281
    GROUP BY session_id) t ON s.id = t.session_id
ORDER BY device;
