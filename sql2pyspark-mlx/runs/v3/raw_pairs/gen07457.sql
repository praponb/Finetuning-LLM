with web_events_by_session as (
    select session_id, count(duration_sec) duration_sec_count
    from web_events
    group by session_id
)
select sessions.country, sessions.customer_id, web_events_by_session.duration_sec_count
from sessions
join web_events_by_session on sessions.id = web_events_by_session.session_id
where web_events_by_session.duration_sec_count > 5;
