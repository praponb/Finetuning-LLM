select * from (select page, event_type, duration_sec from web_events)
pivot (sum(duration_sec) for event_type in ('page_view', 'search'))
order by page
