select
    id,
    session_id,
    event_time,
    rank() over (partition by session_id order by event_time) as rnk
from web_events
where event_type in ('add_to_cart', 'purchase')
order by rnk desc;
