SELECT
    id,
    country,
    started_at,
    PERCENT_RANK() OVER (PARTITION BY country ORDER BY started_at) started_at_pct_rank
FROM sessions
WHERE browser IN ('Chrome', 'Safari')
ORDER BY id DESC
