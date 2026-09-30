select date_trunc('MONTH', event_time) as event_month, page, concat_ws(', ', sort_array(collect_set(event_type))) as event_type_values
from web_events
where duration_sec is not null and (event_time >= '2024-09-28' or event_time >= '2025-06-26')
group by date_trunc('MONTH', event_time), page
