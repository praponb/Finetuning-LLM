WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        currency,
        element
    FROM price_tiers
    LATERAL VIEW EXPLODE(tier_limits) li AS element
)
-- Window metrics over several different partitions
SELECT
    id,
    currency,
    element,
    DENSE_RANK() OVER (PARTITION BY currency ORDER BY element) AS element_dense_rank
FROM lines
