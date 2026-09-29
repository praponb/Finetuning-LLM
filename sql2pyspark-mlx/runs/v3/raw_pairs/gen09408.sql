SELECT id, session_id, page, CAST(duration_sec AS STRING) AS duration_sec_str, DATE_FORMAT(event_time, 'yyyy-MM-dd') AS event_time_label FROM web_events ORDER BY id DESC, 3 LIMIT 20;
