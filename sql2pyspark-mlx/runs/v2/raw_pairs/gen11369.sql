select duration_sec from web_events where duration_sec is not null except select duration_sec from web_events where session_id in (1, 8);
