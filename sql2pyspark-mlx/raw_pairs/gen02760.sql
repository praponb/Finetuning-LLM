SELECT id, duration_sec, event_time, page, YEAR(event_time) AS event_time_year, COALESCE(page, 'N/A') AS page_filled FROM web_events
