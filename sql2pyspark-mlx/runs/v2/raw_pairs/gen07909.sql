SELECT page, SUM(duration_sec) AS total_duration_sec, DENSE_RANK() OVER (ORDER BY SUM(duration_sec) DESC) AS duration_sec_rank
FROM web_events
GROUP BY page
ORDER BY duration_sec_rank DESC;
