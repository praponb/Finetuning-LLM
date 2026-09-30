select id, event_time, duration_sec, case when event_type = 'purchase' then 1 else 0 end as is_purchase from web_events where duration_sec <= 326 and hour(event_time) between 6 and 10;
