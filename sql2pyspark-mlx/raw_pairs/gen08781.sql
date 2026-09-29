select session_id, ceil(duration_sec) duration_sec_whole
from web_events
where event_type in ('add_to_cart', 'purchase', 'search') and page = '/product' and session_id in (6, 15, 18);
