with latest as (
    select *, row_number() over (partition by event_type order by event_time asc nulls last, id asc) as rank_in_group
    from web_events
    qualify rank_in_group = 1
)
select
    s.id,
    s.session_id,
    s.event_type,
    s.page,
    s.event_time,
    s.duration_sec,
    s.rank_in_group,
    (select min(w2.duration_sec) from web_events w2 where w2.session_id = s.session_id) as min_web_event_duration_sec
from latest s
order by id asc
