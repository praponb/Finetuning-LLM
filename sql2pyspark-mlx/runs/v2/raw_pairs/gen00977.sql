SELECT id, page, COUNT(*) OVER (PARTITION BY page) AS page_count FROM web_events;
