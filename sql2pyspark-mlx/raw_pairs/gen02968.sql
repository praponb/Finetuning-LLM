select
    page,
    session_id,
    event_time,
    trim(page) as clean_page
from web_events
where to_date(event_time) < '2023-04-01'
order by session_id desc, page
