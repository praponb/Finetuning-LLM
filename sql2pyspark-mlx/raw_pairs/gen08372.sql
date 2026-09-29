WITH web_events_by_session AS (
    SELECT session_id, SORT_ARRAY(COLLECT_SET(page)) AS page_list, MAX(duration_sec) - MIN(duration_sec) AS duration_sec_range
    FROM web_events
    GROUP BY session_id
)
SELECT
    s.id,
    s.country,
    s.browser,
    webs.page_list,
    webs.duration_sec_range
FROM sessions s
LEFT JOIN web_events_by_session webs ON s.id = webs.session_id
WHERE webs.duration_sec_range > 24;
