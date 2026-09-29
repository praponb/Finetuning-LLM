SELECT id, device, started_at, browser FROM (SELECT id, device, started_at, browser, ROW_NUMBER() OVER (PARTITION BY device ORDER BY started_at DESC, id) AS rn FROM sessions) AS ranked WHERE rn <= 5;
