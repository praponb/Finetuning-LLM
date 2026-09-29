SELECT * FROM (SELECT page, event_type, duration_sec FROM web_events) src PIVOT (MAX(duration_sec) FOR event_type IN ('purchase' AS purchase, 'click' AS click, 'add_to_cart' AS add_to_cart));
