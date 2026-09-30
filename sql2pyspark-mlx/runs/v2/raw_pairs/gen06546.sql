select id, session_id
from web_events
where not (page = '/cart' and event_type in ('page_view', 'purchase', 'search'));
