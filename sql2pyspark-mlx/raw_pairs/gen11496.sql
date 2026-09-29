select
    id,
    event_type,
    event_time,
    year(event_time) as event_time_year,
    concat(page, ' - ', event_type) as page_event_type
from web_events
where page in ('/checkout', '/product') and duration_sec is not null;
