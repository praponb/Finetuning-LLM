WITH ranked AS (SELECT *, RANK() OVER (PARTITION BY page ORDER BY duration_sec) AS row_num FROM web_events) SELECT * FROM ranked WHERE row_num <= 5 ORDER BY session_id DESC
