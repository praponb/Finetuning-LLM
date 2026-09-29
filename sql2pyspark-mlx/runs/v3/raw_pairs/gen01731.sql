with web_events_by_session as (
    select session_id, sum(duration_sec) as total_duration_sec
    from web_events
    where event_type in ('page_view', 'click')
    group by session_id
)
select s.id, s.started_at, webs.total_duration_sec
from sessions s
left join web_events_by_session webs on s.id = webs.session_id
order by 3 desc
