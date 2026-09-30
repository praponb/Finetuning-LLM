select
    s.id,
    s.started_at,
    s.customer_id,
    s.browser
from sessions s
where not exists (select 1 from web_events we where we.session_id = s.id)
