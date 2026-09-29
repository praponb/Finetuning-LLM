select page, event_type, count(distinct duration_sec) as unique_event_types from web_events where page in ('/checkout', '/home') group by cube(page, event_type)
