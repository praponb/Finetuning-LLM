select
    s.country,
    s.browser,
    t.max_duration_sec,
    t.page_view_count
from sessions s
left join (select session_id, max(duration_sec) as max_duration_sec, sum(case when event_type = 'page_view' then 1 else 0 end) as page_view_count
    from web_events
    group by session_id) t on s.id = t.session_id
