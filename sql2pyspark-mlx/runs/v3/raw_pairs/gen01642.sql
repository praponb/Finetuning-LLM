select id, duration_sec, row_number() over (order by duration_sec, id) as row_num from web_events where session_id in (15, 21, 34, 40)
