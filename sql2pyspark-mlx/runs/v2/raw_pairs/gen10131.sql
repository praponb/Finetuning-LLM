SELECT event_type, LOWER(page) AS page_lower, CAST(event_time AS DATE) AS event_time_day FROM web_events WHERE event_time >= '2023-10-01' AND duration_sec >= 238
