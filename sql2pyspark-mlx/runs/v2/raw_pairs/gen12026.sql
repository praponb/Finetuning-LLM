select
    s.browser,
    s.device,
    s.country,
    we.page,
    we.session_id
from sessions s
left outer join web_events we on we.session_id = s.id
order by s.device;
