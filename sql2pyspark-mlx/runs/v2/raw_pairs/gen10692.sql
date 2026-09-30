WITH ranked AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY device ORDER BY started_at, id) AS pos
    FROM sessions
)
SELECT id, device, started_at
FROM ranked
WHERE pos <= 3
ORDER BY id;
