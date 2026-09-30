WITH top_sessions AS (
    SELECT
        id,
        device,
        started_at,
        country,
        ROW_NUMBER() OVER (PARTITION BY device ORDER BY started_at DESC, id) row_num
    FROM sessions
)
SELECT
    id,
    device,
    started_at,
    country
FROM top_sessions
WHERE row_num <= 3;
