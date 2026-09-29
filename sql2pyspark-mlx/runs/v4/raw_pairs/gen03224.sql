SELECT id, page, ROUND(duration_sec * 0.9, 2) AS adjusted_duration_sec, CAST(duration_sec AS STRING) AS duration_sec_str FROM web_events WHERE TO_DATE(event_time) >= '2025-04-01';
