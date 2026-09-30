WITH top_sessions AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY device ORDER BY started_at DESC, id) AS pos
    FROM sessions
)
SELECT id, device, started_at
FROM top_sessions
WHERE pos = 1
ORDER BY device;
