select * from (select page, event_type, duration_sec from web_events) pivot (max(duration_sec) for event_type in ('click', 'purchase', 'search', 'add_to_cart'))
