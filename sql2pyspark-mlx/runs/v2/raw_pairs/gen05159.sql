select
    case when duration_sec >= 540 then 'high' when duration_sec >= 185 then 'mid' else 'low' end as duration_sec_bucket,
    page,
    event_type,
    concat_ws(', ', sort_array(collect_set(page))) as page_values
from web_events
group by case when duration_sec >= 540 then 'high' when duration_sec >= 185 then 'mid' else 'low' end, page, event_type
order by page_values asc;
