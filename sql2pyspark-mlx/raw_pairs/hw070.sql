SELECT id, HOUR(event_time) AS hr, MINUTE(event_time) AS mn, SECOND(event_time) AS sc FROM web_events;
