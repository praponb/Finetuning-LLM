select id, event_type, duration_sec, session_id from (select *, row_number() over (partition by event_type order by duration_sec desc, id) as rnk from web_events) as t where rnk = 1;
