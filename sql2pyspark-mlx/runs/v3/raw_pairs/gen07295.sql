SELECT
    id,
    browser,
    started_at,
    DENSE_RANK() OVER (PARTITION BY browser ORDER BY started_at) AS started_at_dense_rank
FROM sessions
WHERE device IN ('desktop', 'mobile')
