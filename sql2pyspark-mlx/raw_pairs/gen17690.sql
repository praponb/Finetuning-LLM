WITH exploded AS (
    SELECT id, val
    FROM price_tiers
    LATERAL VIEW EXPLODE(tier_limits) e AS val
),
metrics AS (
    SELECT
        id,
        val,
        CUME_DIST() OVER w_recent AS val_cume_dist,
        RANK() OVER w_ordered AS val_rank
    FROM exploded
    WINDOW w_recent AS (ORDER BY val), w_ordered AS (ORDER BY val DESC NULLS LAST)
),
metrics_price_tiers AS (
    SELECT
        *,
        PERCENT_RANK() OVER (ORDER BY val NULLS LAST) AS val_percent_rank,
        DENSE_RANK() OVER w_grp AS val_cume_dist_dense_rank
    FROM metrics
    WINDOW w_grp AS (ORDER BY val_cume_dist)
),
kept_keys AS (
    SELECT id
    FROM metrics_price_tiers
    EXCEPT ALL
    SELECT id
    FROM price_tiers
    WHERE effective_from >= '2025-07-01'
),
verified AS (
    SELECT s.*
    FROM metrics_price_tiers s
    INNER JOIN kept_keys k ON s.id = k.id
),
remaining_keys AS (
    SELECT id
    FROM verified
    EXCEPT DISTINCT
    SELECT id
    FROM price_tiers
    WHERE effective_from <= '2024-09-25'
),
verified_price_tiers AS (
    SELECT s.*
    FROM verified s
    INNER JOIN remaining_keys k ON s.id = k.id
),
base AS (
    SELECT
        *,
        FLOOR(val_percent_rank) AS val_percent_rank_whole,
        CEIL(val_cume_dist_dense_rank) AS val_cume_dist_dense_rank_whole
    FROM verified_price_tiers
    WHERE val_percent_rank <= 0.66
)
SELECT
    CASE WHEN val_cume_dist_dense_rank >= 7 THEN 'PREMIUM' ELSE 'STANDARD' END AS val_cume_dist_dense_rank_tier,
    ROUND(AVG(val_percent_rank), 2) AS avg_val_percent_rank,
    COUNT(DISTINCT id) AS distinct_ids,
    APPROX_PERCENTILE(val_cume_dist, 0.5) AS val_cume_dist_p50,
    GROUPING(CASE WHEN val_cume_dist_dense_rank >= 7 THEN 'PREMIUM' ELSE 'STANDARD' END) AS gid
FROM base
GROUP BY CUBE (CASE WHEN val_cume_dist_dense_rank >= 7 THEN 'PREMIUM' ELSE 'STANDARD' END);
