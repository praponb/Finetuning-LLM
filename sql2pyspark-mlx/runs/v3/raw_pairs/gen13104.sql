select page, round(stddev(duration_sec), 2) as stddev_duration_sec, count(case when event_type = 'add_to_cart' then 1 end) as add_to_cart_count
from web_events
group by page
having add_to_cart_count >= 5;
