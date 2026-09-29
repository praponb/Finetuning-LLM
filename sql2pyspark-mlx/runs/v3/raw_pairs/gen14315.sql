select duration_sec from web_events where event_type in ('click', 'add_to_cart') or event_type = 'click' order by duration_sec;
