WITH ranked_sessions AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY device ORDER BY started_at DESC, id) AS row_num
    FROM sessions
)
SELECT
    id,
    device,
    started_at,
    country
FROM ranked_sessions
WHERE row_num <= 2
ORDER BY device;
