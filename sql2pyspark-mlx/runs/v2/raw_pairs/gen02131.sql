SELECT * FROM (SELECT id, page, duration_sec, event_type, RANK() OVER (PARTITION BY page ORDER BY duration_sec) AS rn FROM web_events WHERE duration_sec != 251) AS t WHERE rn <= 2 ORDER BY rn DESC;
