select * from (select page, event_type, duration_sec from web_events) pivot (avg(duration_sec) for event_type in ('add_to_cart', 'click', 'page_view'));
