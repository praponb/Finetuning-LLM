with top_web_events as (select *, row_number() over (partition by event_type order by duration_sec, id) as rnk from web_events) select * from top_web_events where rnk <= 3;
