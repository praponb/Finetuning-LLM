select count(*) as total_count, count(*) as cnt from web_events where event_type not in ('add_to_cart', 'click');
