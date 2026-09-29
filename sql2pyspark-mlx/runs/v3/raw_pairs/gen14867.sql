SELECT page, SUM(duration_sec) AS total_duration_sec, DENSE_RANK() OVER (ORDER BY SUM(duration_sec) DESC) AS rnk FROM web_events GROUP BY page ORDER BY page, rnk DESC;
