SELECT TO_DATE(event_time) AS event_day, ROUND(STDDEV(duration_sec), 2) AS stddev_duration_sec FROM web_events GROUP BY 1;
