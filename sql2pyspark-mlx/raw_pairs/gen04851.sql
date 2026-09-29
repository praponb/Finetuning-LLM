select id, session_id, duration_sec from (select *, row_number() over (partition by session_id order by duration_sec desc, id) as pos from web_events) as r where pos <= 3 order by session_id desc, id
