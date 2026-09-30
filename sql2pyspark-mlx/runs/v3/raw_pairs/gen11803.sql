with web_events_by_session as (
    select session_id, sum(duration_sec) as total_duration_sec, count(duration_sec) as duration_sec_count
    from web_events
    group by session_id
)
select
    sessions.customer_id,
    sessions.started_at,
    web_events_by_session.total_duration_sec,
    web_events_by_session.duration_sec_count
from sessions
left join web_events_by_session on sessions.id = web_events_by_session.session_id;
