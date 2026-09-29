SELECT event_time, duration_sec, LENGTH(page) AS page_length FROM web_events WHERE page IN ('/search', '/checkout', '/cart')
