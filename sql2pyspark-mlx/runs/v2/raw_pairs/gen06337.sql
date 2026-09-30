SELECT id, event_type, event_time, SUBSTRING(page, 1, 3) AS page_prefix FROM web_events WHERE duration_sec BETWEEN 250 AND 268 AND page != '/product';
