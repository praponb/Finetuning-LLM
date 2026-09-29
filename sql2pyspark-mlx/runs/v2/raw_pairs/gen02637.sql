SELECT duration_sec, CONCAT(page, ' - ', event_type) AS page_event_type, CONCAT(page, ' ', event_type) AS page_event_type_2 FROM web_events WHERE event_type = 'search';
