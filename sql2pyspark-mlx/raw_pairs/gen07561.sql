WITH ranked AS (
    SELECT
        id,
        device,
        started_at,
        country,
        ROW_NUMBER() OVER (PARTITION BY device ORDER BY started_at, id) AS rn
    FROM sessions
)
SELECT *
FROM ranked
WHERE rn <= 5;
