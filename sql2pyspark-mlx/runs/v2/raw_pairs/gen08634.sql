select max(duration_sec) as max_duration_sec
from web_events
where page in ('/search', '/cart', '/checkout') and page in ('/cart', '/product')
order by max_duration_sec desc;
