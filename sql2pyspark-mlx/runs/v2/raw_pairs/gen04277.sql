select id, event_time, page from web_events where duration_sec <> 288 order by page desc, id;
