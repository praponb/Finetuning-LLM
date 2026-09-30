SELECT
    id,
    duration_sec,
    session_id,
    page,
    CAST(duration_sec AS STRING) duration_sec_str
FROM web_events
WHERE event_type NOT IN ('add_to_cart', 'click');
