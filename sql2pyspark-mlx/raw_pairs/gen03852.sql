SELECT event_type, MIN(event_time) AS first_event_time FROM web_events WHERE event_type IN ('page_view', 'purchase') AND TO_DATE(event_time) >= '2023-04-01' GROUP BY event_type;
