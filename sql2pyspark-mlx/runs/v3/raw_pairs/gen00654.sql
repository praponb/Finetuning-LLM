SELECT id, event_type, UPPER(event_type) AS event_type_upper FROM web_events WHERE event_type IN ('purchase', 'page_view', 'search')
