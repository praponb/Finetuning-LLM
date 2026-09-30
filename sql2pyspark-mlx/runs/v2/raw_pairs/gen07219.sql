select
    s.started_at,
    s.device,
    t.pct_purchase,
    t.avg_duration_sec
from sessions s
inner join (select session_id, round((100 * sum(case when event_type = 'purchase' then 1 else 0 end)) / count(*), 1) as pct_purchase, round(avg(duration_sec), 2) as avg_duration_sec
    from web_events
    group by session_id) t on s.id = t.session_id
