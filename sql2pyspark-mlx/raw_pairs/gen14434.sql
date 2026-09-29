WITH web_events_by_session AS (
    SELECT session_id, COUNT(CASE WHEN page = '/search' THEN 1 END) AS search_count
    FROM web_events
    GROUP BY session_id
)
SELECT s.country, webs.search_count
FROM sessions s
INNER JOIN web_events_by_session webs ON s.id = webs.session_id
