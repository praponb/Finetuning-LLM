SELECT id, browser, started_at FROM (SELECT id, browser, started_at, DENSE_RANK() OVER (PARTITION BY browser ORDER BY started_at DESC) AS pos FROM sessions) AS r WHERE pos = 1 ORDER BY id;
