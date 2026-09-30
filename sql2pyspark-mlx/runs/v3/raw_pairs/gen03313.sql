select id, page, duration_sec, event_type from (select *, rank() over (partition by page order by duration_sec) as rnk from web_events) as ranked where rnk <= 3 order by duration_sec
