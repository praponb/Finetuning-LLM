SELECT
    id,
    country,
    started_at,
    NTILE(10) OVER (PARTITION BY country ORDER BY started_at, id) AS bucket
FROM sessions;
