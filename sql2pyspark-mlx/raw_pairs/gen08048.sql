select id, event_type, event_time, rank() over (partition by event_type order by event_time) as event_time_rank from web_events where event_type not in ('add_to_cart', 'search', 'purchase');
