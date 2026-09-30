SELECT * FROM (SELECT *, RANK() OVER (PARTITION BY page ORDER BY event_time DESC) AS row_num FROM web_events) AS t WHERE row_num <= 2
