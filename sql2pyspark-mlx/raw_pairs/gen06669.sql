WITH top_sessions AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY device ORDER BY started_at, id) AS rn
    FROM sessions
    WHERE TO_DATE(started_at) < '2025-06-01'
)
SELECT id, device, started_at
FROM top_sessions
WHERE rn <= 3;
