select s.id, count(we.id) web_event_count
from sessions s
inner join web_events we on we.session_id = s.id
where we.duration_sec != 309
group by s.id
order by web_event_count, s.id desc
limit 20;
