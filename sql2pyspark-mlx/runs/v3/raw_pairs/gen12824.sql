SELECT *
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY device ORDER BY started_at, id) AS pos
    FROM sessions
    WHERE browser = 'Safari'
) AS ranked
WHERE pos <= 2
ORDER BY started_at DESC
