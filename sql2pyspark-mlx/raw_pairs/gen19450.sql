SELECT *, INITCAP(TRIM(event_type)) event_type_title, CONCAT_WS('-', event_type, page) event_type_page_key FROM web_events WHERE event_time < '2023-07-01';
