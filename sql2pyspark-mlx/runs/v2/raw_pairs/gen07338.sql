SELECT page, event_type, FLOOR(duration_sec) AS duration_sec_whole, INITCAP(TRIM(page)) AS page_title FROM web_events;
