SELECT page, duration_sec_count FROM (SELECT page, COUNT(duration_sec) AS duration_sec_count FROM web_events GROUP BY page) AS s WHERE duration_sec_count > 9;
