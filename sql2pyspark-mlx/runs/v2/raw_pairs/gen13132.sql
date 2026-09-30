select
    s.id,
    s.country,
    t.avg_duration_sec,
    t.product_duration_sec
from sessions s
left join (select session_id, round(avg(duration_sec), 2) as avg_duration_sec, sum(case when page = '/product' then duration_sec else 0 end) as product_duration_sec
    from web_events
    where session_id in (4, 17, 36)
    group by session_id) t on s.id = t.session_id
order by product_duration_sec
