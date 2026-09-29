select distinct event_type, page
from web_events
where page = '/checkout';
