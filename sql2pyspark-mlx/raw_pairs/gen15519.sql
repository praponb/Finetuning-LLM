WITH lines AS (
    -- Unnest the array into one row per element
    SELECT id, val
    FROM price_tiers
    LATERAL VIEW EXPLODE(tier_limits) t AS val
),
scored AS (
    -- Running and moving aggregates
    SELECT
        *,
        RANK() OVER (ORDER BY val DESC NULLS LAST) AS val_rank,
        CUME_DIST() OVER w_hist AS val_cume_dist,
        CUME_DIST() OVER (ORDER BY val DESC) AS val_cume_dist_2
    FROM lines
    WINDOW w_hist AS (ORDER BY val DESC NULLS FIRST)
),
scored_price_tiers AS (
    -- Ranking and gap features
    SELECT
        *,
        RANK() OVER (ORDER BY val DESC NULLS LAST) AS val_rank_2,
        DENSE_RANK() OVER (ORDER BY val) AS val_dense_rank,
        PERCENT_RANK() OVER (ORDER BY val_rank NULLS FIRST) AS val_rank_percent_rank,
        DENSE_RANK() OVER (ORDER BY val_cume_dist DESC) AS val_cume_dist_dense_rank
    FROM scored
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        val,
        val_rank,
        val_cume_dist,
        val_cume_dist_2,
        val_rank_2,
        val_dense_rank,
        RANK() OVER (ORDER BY val DESC NULLS LAST) AS val_rank_3
    FROM scored_price_tiers
    WHERE val_rank_percent_rank < 0.62
)
-- Window metrics over several different partitions
SELECT
    *,
    DENSE_RANK() OVER (ORDER BY val_rank_2 NULLS FIRST) AS val_rank_2_dense_rank,
    RANK() OVER (ORDER BY val_dense_rank NULLS LAST) AS val_dense_rank_rank
FROM metrics;
