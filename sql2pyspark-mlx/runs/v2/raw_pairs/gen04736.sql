select * from (select event_type, page, duration_sec from web_events)
pivot (max(duration_sec) for page in ('/search', '/cart'))
order by event_type
