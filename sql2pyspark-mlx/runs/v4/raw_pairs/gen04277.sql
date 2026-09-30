select id, page, event_time, dense_rank() over (partition by page order by event_time) as dense_rnk from web_events;
