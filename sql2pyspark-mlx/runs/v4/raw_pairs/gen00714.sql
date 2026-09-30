SELECT *
FROM (
    SELECT
        session_id,
        event_type,
        duration_sec
    FROM web_events
) src
PIVOT (
    AVG(duration_sec)
    FOR event_type IN ('add_to_cart', 'search')
);
