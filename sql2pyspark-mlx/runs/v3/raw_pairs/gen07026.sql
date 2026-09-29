SELECT page, ROUND(STDDEV(duration_sec), 2) AS stddev_duration_sec FROM web_events GROUP BY page;
