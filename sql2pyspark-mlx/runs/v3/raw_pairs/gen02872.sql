select
    s.id,
    s.country,
    s.device,
    s.started_at
from sessions s
where s.started_at < '2025-10-27' and not exists (select 1 from web_events we where we.session_id = s.id)
order by s.started_at desc, device;
