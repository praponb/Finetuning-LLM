select month(s.started_at) as started_month, count(we.duration_sec) as duration_sec_count
from web_events we
left join sessions s on we.session_id = s.id
group by month(s.started_at)
having count(we.duration_sec) >= 5
