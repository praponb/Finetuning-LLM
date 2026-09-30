with session_summary as (
    select session_id, sort_array(collect_set(event_type)) as event_type_list
    from web_events
    group by session_id
)
select s.id, s.device, ss.event_type_list
from sessions s
join session_summary ss on s.id = ss.session_id;
