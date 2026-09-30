select
    s.id,
    s.country,
    count(we.id) as web_event_count,
    max(we.duration_sec) as max_duration_sec,
    sum(case when we.page = '/product' then 1 else 0 end) as product_count
from sessions s
left outer join web_events we on we.session_id = s.id
group by s.id, s.country
having count(we.id) >= 2;
