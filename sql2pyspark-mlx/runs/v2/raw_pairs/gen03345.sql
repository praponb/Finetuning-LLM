select *, date_format(event_time, 'MMM yyyy') as event_time_label
from web_events
where event_type not in ('purchase', 'search', 'add_to_cart') and session_id in (2, 6, 33)
order by session_id desc;
