SELECT id, device, started_at
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY device ORDER BY started_at DESC, id) AS rnk
    FROM sessions
) AS ranked
WHERE rnk = 1
ORDER BY started_at, id DESC
