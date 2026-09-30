select
    we.session_id,
    s.browser,
    s.started_at,
    concat(we.page, ' - ', we.event_type) as page_event_type
from web_events we
left join sessions s on s.id = we.session_id
where we.page is not null
order by s.browser desc, we.session_id;
