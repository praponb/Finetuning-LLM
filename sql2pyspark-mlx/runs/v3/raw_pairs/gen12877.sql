select id, page, duration_sec from (select id, page, duration_sec, row_number() over (partition by page order by duration_sec, id) as rn from web_events) as t where rn <= 1
