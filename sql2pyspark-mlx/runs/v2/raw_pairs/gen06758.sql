WITH ranked AS (
    SELECT *, DENSE_RANK() OVER (PARTITION BY country ORDER BY started_at DESC) AS row_num
    FROM sessions
)
SELECT id, country, started_at
FROM ranked
WHERE row_num = 1;
