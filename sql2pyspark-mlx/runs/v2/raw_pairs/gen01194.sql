SELECT
    id,
    country,
    started_at,
    RANK() OVER (PARTITION BY country ORDER BY started_at) AS rnk
FROM sessions
WHERE browser IN ('Safari', 'Chrome')
