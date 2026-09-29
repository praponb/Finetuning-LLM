SELECT session_id, MAX(duration_sec) - MIN(duration_sec) AS duration_sec_range FROM web_events WHERE duration_sec <> 459 GROUP BY session_id
