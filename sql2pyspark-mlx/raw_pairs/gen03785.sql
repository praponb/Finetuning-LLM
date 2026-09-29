with web_events_by_session as (
    select session_id, min(duration_sec) as min_duration_sec
    from web_events
    group by session_id
)
select s.id, s.device, webs.min_duration_sec
from sessions s
inner join web_events_by_session webs on s.id = webs.session_id
where webs.min_duration_sec > 450
order by device, id
limit 20;
