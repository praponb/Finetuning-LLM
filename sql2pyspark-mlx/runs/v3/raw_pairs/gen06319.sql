select
    s.id,
    s.customer_id,
    s.browser,
    s.started_at
from sessions s
where exists (select 1 from web_events we where we.session_id = s.id and we.duration_sec < 458)
