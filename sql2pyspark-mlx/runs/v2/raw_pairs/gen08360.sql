SELECT
    id,
    device,
    started_at,
    DENSE_RANK() OVER (PARTITION BY device ORDER BY started_at DESC) dense_rnk
FROM sessions
WHERE country IS NOT NULL
ORDER BY device DESC
