SELECT * FROM (SELECT session_id, event_type, duration_sec FROM web_events) src PIVOT (SUM(duration_sec) AS value_sum, COUNT(1) AS item_count FOR event_type IN ('add_to_cart', 'click'))
