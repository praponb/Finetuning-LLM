select * from (select page, event_type, duration_sec from web_events) pivot (sum(duration_sec) for event_type in ('search', 'page_view', 'click')) order by page
