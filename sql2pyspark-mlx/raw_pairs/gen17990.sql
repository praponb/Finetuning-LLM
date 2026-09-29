select
    s.id,
    s.device,
    s.browser,
    s.started_at,
    s.country,
    latest.web_event_duration_sec
from sessions s
join lateral (
        select c.duration_sec as web_event_duration_sec
        from web_events c
        where c.session_id = s.id
          and c.event_time <= s.started_at
        order by c.event_time desc, c.id desc
        limit 1
    ) latest on true
