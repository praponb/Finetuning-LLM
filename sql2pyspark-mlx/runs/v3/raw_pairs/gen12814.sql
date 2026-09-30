SELECT id, event_time, CAST(duration_sec AS DOUBLE) AS duration_sec_str, COALESCE(duration_sec, 0) AS duration_sec_filled FROM web_events WHERE session_id IN (17, 27) AND page != '/home';
