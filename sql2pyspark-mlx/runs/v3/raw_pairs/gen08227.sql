SELECT
    id,
    customer_id,
    started_at,
    country,
    PERCENT_RANK() OVER (PARTITION BY customer_id ORDER BY started_at) AS started_at_pct_rank,
    ROW_NUMBER() OVER (PARTITION BY country ORDER BY started_at, id) AS rn
FROM sessions
WHERE customer_id IS NULL;
