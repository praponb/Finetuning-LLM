WITH top_sessions AS (
    SELECT
        id,
        device,
        started_at,
        RANK() OVER (PARTITION BY device ORDER BY started_at) AS rnk
    FROM sessions
)
SELECT *
FROM top_sessions
WHERE rnk <= 1
ORDER BY id DESC, device DESC;
