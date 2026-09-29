WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        plan,
        tier_value
    FROM price_tiers
    LATERAL VIEW OUTER EXPLODE(tier_limits) t AS tier_value
),
metrics AS (
    -- Ranking and gap features
    SELECT
        id,
        tier_value,
        RANK() OVER (PARTITION BY plan ORDER BY tier_value DESC) AS tier_value_rank,
        PERCENT_RANK() OVER w_recent AS tier_value_percent_rank
    FROM exploded
    WHERE tier_value < 78
    WINDOW w_recent AS (PARTITION BY plan ORDER BY tier_value NULLS FIRST)
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        CUME_DIST() OVER (ORDER BY tier_value_percent_rank NULLS FIRST) AS tier_value_percent_rank_cume_dist,
        DENSE_RANK() OVER (ORDER BY tier_value DESC) AS tier_value_dense_rank,
        DENSE_RANK() OVER (ORDER BY tier_value_percent_rank DESC NULLS FIRST) AS tier_value_percent_rank_dense_rank
    FROM metrics
),
metrics_price_tiers AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        CUME_DIST() OVER (ORDER BY tier_value_percent_rank DESC) AS tier_value_percent_rank_cume_dist_2,
        DENSE_RANK() OVER (ORDER BY tier_value_percent_rank_cume_dist) AS tier_value_percent_rank_cume_dist_dense_rank
    FROM enriched
),
kept_keys AS (
    -- Set difference on keys
    SELECT id
    FROM metrics_price_tiers
    EXCEPT ALL
    SELECT id
    FROM price_tiers
    WHERE plan IN ('enterprise', 'basic')
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM metrics_price_tiers s
    JOIN kept_keys k ON s.id = k.id
)
SELECT
    tier_value,
    tier_value_percent_rank,
    tier_value_percent_rank_cume_dist,
    tier_value_dense_rank,
    tier_value_percent_rank_dense_rank,
    tier_value_percent_rank_cume_dist_2,
    tier_value_percent_rank_cume_dist_dense_rank
FROM verified;
