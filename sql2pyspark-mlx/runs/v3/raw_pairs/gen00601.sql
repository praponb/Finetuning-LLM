select
    page,
    concat_ws(', ', sort_array(collect_set(page))) as page_values,
    max(event_time) as latest_event_time,
    count(*) as row_count
from web_events
where hour(event_time) between 7 and 10 and duration_sec >= 331
group by page
having count(*) < 1
