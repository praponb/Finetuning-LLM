select id, page, event_time from (select id, page, event_time, row_number() over (partition by page order by event_time, id) rn from web_events) t where rn <= 5;
