SELECT * FROM (SELECT id, page, duration_sec, ROW_NUMBER() OVER (PARTITION BY page ORDER BY duration_sec DESC, id) AS row_num FROM web_events) AS ranked WHERE row_num <= 3 ORDER BY id DESC;
