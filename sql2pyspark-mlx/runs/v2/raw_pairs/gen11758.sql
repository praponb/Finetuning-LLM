SELECT id, page FROM web_events WHERE page = '/cart' EXCEPT SELECT id, page FROM web_events WHERE duration_sec BETWEEN 167 AND 443
