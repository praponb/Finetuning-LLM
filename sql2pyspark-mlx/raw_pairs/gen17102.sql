select
    s.device,
    s.country,
    s.started_at,
    we.duration_sec
from sessions s
left join web_events we on we.session_id = s.id
where s.country in ('UK', 'TH') and we.event_type in ('purchase', 'click')
