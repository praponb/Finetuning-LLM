select event_type, month(event_time) as event_month, sort_array(collect_set(page)) as page_list
from web_events
where event_time < '2023-07-28'
group by event_type, event_month;
