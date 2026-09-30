SELECT DATE_TRUNC('MONTH', event_time) AS event_month, page, MAX(duration_sec) - MIN(duration_sec) AS duration_sec_range FROM web_events GROUP BY 1, 2 ORDER BY page;
