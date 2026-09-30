select s.id, s.started_at, s.device
from sessions s
where s.country in ('DE', 'UK') and exists (select 1 from web_events we where we.session_id = s.id);
