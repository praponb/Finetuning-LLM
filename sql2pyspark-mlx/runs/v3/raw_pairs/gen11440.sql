select s.id, s.device, count(we.id) as web_event_count
from sessions s
left join web_events we on s.id = we.session_id
where we.duration_sec between 369 and 416
group by s.id, s.device
