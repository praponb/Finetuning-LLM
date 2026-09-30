SELECT *, CAST(duration_sec AS STRING) AS duration_sec_str FROM web_events WHERE HOUR(event_time) BETWEEN 12 AND 16 AND duration_sec = 388;
