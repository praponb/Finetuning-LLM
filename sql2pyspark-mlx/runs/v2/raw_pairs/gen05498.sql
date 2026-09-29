select s.browser, max(we.event_time) as last_event_time, count(we.duration_sec) as duration_sec_count
from web_events we
right join sessions s on we.session_id = s.id
where s.country in ('DE', 'UK')
group by s.browser;
