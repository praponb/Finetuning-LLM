SELECT *, TO_DATE(event_time) AS event_time_date, TO_DATE(event_time) AS event_time_date_2 FROM web_events WHERE event_type = 'add_to_cart';
