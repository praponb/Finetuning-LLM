SELECT * FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY device ORDER BY started_at, id) AS pos FROM sessions) AS r WHERE pos <= 1 ORDER BY id;
