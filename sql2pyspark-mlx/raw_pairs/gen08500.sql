with session_summary as (
    select session_id, concat_ws(', ', sort_array(collect_set(page))) as page_values, sum(duration_sec) as total_duration_sec
    from web_events
    where hour(event_time) between 12 and 16
    group by session_id
)
select
    s.customer_id,
    s.device,
    ss.page_values,
    ss.total_duration_sec
from sessions s
join session_summary ss on s.id = ss.session_id
order by ss.page_values;
