SELECT id, session_id, UPPER(page) AS page_upper
FROM web_events
ORDER BY 2 DESC, id
LIMIT 3;
