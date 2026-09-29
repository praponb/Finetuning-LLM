select *, lower(page) as page_lower
from web_events
where duration_sec <= 121 and event_type in ('search', 'page_view')
