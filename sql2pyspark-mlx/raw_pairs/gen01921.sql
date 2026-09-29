SELECT id, browser, started_at FROM (SELECT id, browser, started_at, DENSE_RANK() OVER (PARTITION BY browser ORDER BY started_at) rnk FROM sessions) r WHERE rnk <= 2 ORDER BY browser;
