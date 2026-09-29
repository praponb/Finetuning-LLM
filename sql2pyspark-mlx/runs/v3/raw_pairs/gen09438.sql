select event_type, avg(duration_sec) as avg_duration_sec, sort_array(collect_set(page)) as page_list
from web_events
where event_type in ('search', 'page_view', 'click') or session_id in (12, 17, 19, 31)
group by event_type
order by event_type
limit 5;
