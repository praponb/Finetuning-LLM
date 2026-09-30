select duration_sec, coalesce(duration_sec, 0) as duration_sec_filled from web_events where (duration_sec between 239 and 292 and event_time < '2025-01-03') or event_type = 'page_view';
