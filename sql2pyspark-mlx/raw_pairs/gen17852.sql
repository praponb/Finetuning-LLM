WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        plan,
        currency,
        EXPLODE_OUTER(tier_limits) AS val
    FROM price_tiers
)
-- Running and moving aggregates
SELECT *, DENSE_RANK() OVER (PARTITION BY currency ORDER BY val DESC) AS val_dense_rank
FROM flattened
WHERE val > 73
