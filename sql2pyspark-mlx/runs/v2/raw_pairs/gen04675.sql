SELECT
    id,
    device,
    started_at,
    PERCENT_RANK() OVER (PARTITION BY device ORDER BY started_at) AS started_at_pct_rank
FROM sessions
ORDER BY 1;
