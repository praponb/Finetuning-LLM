select sum(case when event_type = 'page_view' then 1 else 0 end) as page_view_count, max(event_time) as latest_event_time, count(case when page = '/checkout' then 1 end) as checkout_count
from web_events
where session_id = 24 and page != '/home'
order by page_view_count desc
