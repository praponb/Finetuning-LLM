SELECT *, DATE_FORMAT(event_time, 'yyyy-MM') event_time_label, DAY(event_time) event_time_day FROM web_events WHERE event_time < '2023-11-18'
