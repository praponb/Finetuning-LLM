SELECT
    event_type,
    page,
    duration_sec,
    CASE WHEN page IN ('/home', '/cart') THEN 'Core' ELSE 'Other' END AS page_group
FROM web_events
WHERE duration_sec BETWEEN 125 AND 187;
