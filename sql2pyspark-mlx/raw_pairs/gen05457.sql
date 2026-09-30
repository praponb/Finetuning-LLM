WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        currency,
        EXPLODE_OUTER(tier_limits) AS element
    FROM price_tiers
)
-- Running and moving aggregates
SELECT
    id,
    currency,
    CUME_DIST() OVER (PARTITION BY currency ORDER BY element DESC NULLS FIRST) AS element_cume_dist,
    RANK() OVER (PARTITION BY currency ORDER BY element DESC NULLS FIRST) AS element_rank,
    SUM(element) OVER (PARTITION BY currency) AS currency_sum_element
FROM exploded;
