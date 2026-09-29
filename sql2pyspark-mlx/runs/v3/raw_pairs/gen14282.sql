SELECT
    id,
    event_time,
    session_id,
    CASE WHEN event_type IN ('purchase', 'add_to_cart') THEN 'Priority' ELSE 'Other' END AS event_type_group,
    COALESCE(duration_sec, 0) AS duration_sec_filled
FROM web_events
WHERE event_time >= '2024-04-01' AND duration_sec <> 276 AND event_type = 'add_to_cart'
