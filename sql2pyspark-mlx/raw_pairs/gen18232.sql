select page, max(duration_sec) as max_duration_sec from web_events where event_type = 'add_to_cart' group by page order by page desc
