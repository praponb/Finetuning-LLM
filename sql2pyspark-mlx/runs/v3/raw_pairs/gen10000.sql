select id, duration_sec from web_events where duration_sec <= 218 except select id, duration_sec from web_events where event_type in ('click', 'add_to_cart')
