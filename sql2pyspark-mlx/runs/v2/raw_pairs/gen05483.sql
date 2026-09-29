select id, page from web_events where duration_sec is not null union all select id, page from web_events where hour(event_time) between 8 and 14 order by id
