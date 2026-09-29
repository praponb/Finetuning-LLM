select event_type, page, count(duration_sec) duration_sec_count from web_events group by cube(event_type, page) order by duration_sec_count asc
