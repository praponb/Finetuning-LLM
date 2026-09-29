select id, session_id, event_time from (select *, rank() over (partition by session_id order by event_time desc) rn from web_events) ranked where rn <= 3;
