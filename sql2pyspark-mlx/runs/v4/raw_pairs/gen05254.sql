select event_time, lower(page) as page_lower
from web_events
where hour(event_time) between 7 and 10 or hour(event_time) between 10 and 17;
