SELECT DATE_TRUNC('MONTH', event_time) AS event_month, AVG(duration_sec) AS avg_duration_sec FROM web_events GROUP BY 1 HAVING SUM(duration_sec) < 1300 ORDER BY 1
