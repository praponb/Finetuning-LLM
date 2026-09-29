select
    id,
    page,
    event_type,
    substring(page, 1, 3) as page_prefix
from web_events
where session_id in (25, 37, 40) or event_type in ('page_view', 'click', 'purchase')
order by page_prefix, id
limit 5;
