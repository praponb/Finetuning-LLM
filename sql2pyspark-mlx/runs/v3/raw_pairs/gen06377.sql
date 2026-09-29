SELECT
    id,
    event_type,
    duration_sec,
    CASE WHEN event_type = 'search' THEN 1 ELSE 0 END AS is_search
FROM web_events
WHERE page = '/cart'
ORDER BY is_search;
