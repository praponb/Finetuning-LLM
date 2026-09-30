WITH RankedSessions AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY browser ORDER BY started_at DESC, id) AS pos
    FROM sessions
)
SELECT
    id,
    browser,
    started_at,
    device
FROM RankedSessions
WHERE pos <= 2
ORDER BY started_at, id
