WITH top_sessions AS (
    SELECT *, RANK() OVER (PARTITION BY device ORDER BY started_at DESC) AS rn
    FROM sessions
)
SELECT
    id,
    device,
    started_at,
    country
FROM top_sessions
WHERE rn <= 1
ORDER BY 2 DESC, id DESC
