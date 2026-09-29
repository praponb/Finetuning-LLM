SELECT
    id,
    device,
    started_at,
    LAG(started_at, 1) OVER (PARTITION BY device ORDER BY started_at, id) AS prev_started_at,
    ROW_NUMBER() OVER (ORDER BY started_at DESC, id) AS row_num
FROM sessions
ORDER BY 5 DESC;
