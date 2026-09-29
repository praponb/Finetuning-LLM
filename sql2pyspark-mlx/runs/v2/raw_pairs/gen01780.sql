with SessionSummary as (
    select session_id, concat_ws(', ', sort_array(collect_set(event_type))) as event_type_values
    from web_events
    group by session_id
)
select s.browser, S.event_type_values
from sessions s
join SessionSummary S on s.id = S.session_id
