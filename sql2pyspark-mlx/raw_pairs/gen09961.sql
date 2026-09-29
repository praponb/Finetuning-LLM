select
    s.id,
    s.device,
    s.browser,
    t.cnt,
    t.purchase_duration_sec
from sessions s
inner join (select session_id, count(*) as cnt, sum(case when event_type = 'purchase' then duration_sec else 0 end) as purchase_duration_sec
    from web_events
    where duration_sec is not null
    group by session_id) t on s.id = t.session_id
where t.cnt >= 7;
