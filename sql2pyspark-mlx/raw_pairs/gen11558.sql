SELECT session_id, CASE WHEN event_type IN ('add_to_cart', 'purchase') THEN 'Tier 1' ELSE 'Other' END AS event_type_group FROM web_events
