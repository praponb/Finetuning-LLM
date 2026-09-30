SELECT *, CASE WHEN duration_sec >= 439 THEN 'High' WHEN duration_sec >= 86 THEN 'Medium' ELSE 'Low' END AS duration_sec_band, FLOOR(duration_sec) AS duration_sec_whole FROM web_events ORDER BY page
