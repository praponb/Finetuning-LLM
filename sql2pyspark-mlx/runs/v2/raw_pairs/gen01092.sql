SELECT
    id,
    page,
    session_id,
    ROUND(duration_sec, 1) AS duration_sec_rounded
FROM web_events
WHERE event_type IN ('search', 'add_to_cart', 'click')
ORDER BY duration_sec_rounded DESC;
