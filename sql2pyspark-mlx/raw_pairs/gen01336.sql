SELECT
    id,
    browser,
    started_at,
    RANK() OVER (PARTITION BY browser ORDER BY started_at) AS rnk
FROM sessions
WHERE customer_id IS NOT NULL;
