SELECT TO_DATE(event_time) event_day, SUM(duration_sec) total_duration_sec, DENSE_RANK() OVER (ORDER BY SUM(duration_sec) DESC) AS rnk FROM web_events GROUP BY TO_DATE(event_time)
