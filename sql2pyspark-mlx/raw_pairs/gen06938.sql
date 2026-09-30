select id, duration_sec, case when event_type in ('click', 'search') then 'Priority' else 'Other' end event_type_group
from web_events
where page = '/home' and page = '/checkout' and page not in ('/cart', '/product', '/search')
order by duration_sec desc, id desc;
