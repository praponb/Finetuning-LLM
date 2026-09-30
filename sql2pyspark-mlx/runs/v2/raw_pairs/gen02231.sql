select
    s.id,
    s.customer_id,
    s.country,
    s.started_at
from sessions s
where not exists (select 1 from web_events we where we.session_id = s.id and we.event_type in ('search', 'purchase'))
order by s.id desc
limit 5
