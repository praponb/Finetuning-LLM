SELECT session_id, SUM(duration_sec) AS total_duration_sec, DENSE_RANK() OVER (ORDER BY SUM(duration_sec) DESC) AS rnk FROM web_events WHERE duration_sec <> 412 GROUP BY session_id
