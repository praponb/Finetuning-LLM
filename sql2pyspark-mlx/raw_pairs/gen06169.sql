select
    we.id,
    we.duration_sec,
    we.event_time,
    s.country,
    concat_ws('/', we.event_type, we.page) as event_type_page_key
from web_events we
full outer join sessions s on we.session_id = s.id
where we.session_id in (8, 14, 15, 36)
order by event_time desc
