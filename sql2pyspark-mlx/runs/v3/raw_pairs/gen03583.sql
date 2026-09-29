select id, session_id, page, duration_sec from web_events where to_date(event_time) >= '2024-08-02' and duration_sec between 236 and 456 order by session_id desc
