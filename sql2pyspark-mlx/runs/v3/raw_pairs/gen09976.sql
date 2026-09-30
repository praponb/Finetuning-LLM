WITH Ranked AS (SELECT *, ROW_NUMBER() OVER (PARTITION BY browser ORDER BY started_at DESC, id) AS rn FROM sessions) SELECT id, browser, started_at, country FROM Ranked WHERE rn <= 3;
