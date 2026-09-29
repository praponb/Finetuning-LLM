SELECT
    id,
    session_id,
    event_type,
    duration_sec,
    INITCAP(TRIM(page)) page_title
FROM web_events
ORDER BY 5 DESC;
