SELECT
    id,
    event_type,
    session_id,
    CONCAT(page, ' ', event_type) AS page_event_type
FROM web_events
WHERE event_time >= '2023-10-13';
