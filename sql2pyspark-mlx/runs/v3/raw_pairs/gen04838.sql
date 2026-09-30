select sum(case when event_type = 'add_to_cart' then duration_sec else 0 end) as add_to_cart_duration_sec
from web_events;
