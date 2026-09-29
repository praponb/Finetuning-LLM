WITH numbered AS (SELECT *, DENSE_RANK() OVER (PARTITION BY page ORDER BY event_time) AS row_num FROM web_events) SELECT * FROM numbered WHERE row_num <= 3 ORDER BY duration_sec DESC;
