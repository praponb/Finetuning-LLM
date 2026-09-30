WITH ranked AS (
    SELECT
        id,
        country,
        started_at,
        ROW_NUMBER() OVER (PARTITION BY country ORDER BY started_at DESC, id) AS pos
    FROM sessions
    WHERE customer_id = 31
)
SELECT id, country, started_at
FROM ranked
WHERE pos <= 3
ORDER BY started_at
