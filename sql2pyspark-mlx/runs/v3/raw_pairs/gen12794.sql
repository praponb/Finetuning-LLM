select id, event_time, row_number() over (order by event_time desc, id) as rn from web_events where duration_sec between 194 and 324
