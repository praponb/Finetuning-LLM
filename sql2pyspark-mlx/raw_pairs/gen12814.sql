WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        plan,
        effective_from,
        currency,
        EXPLODE(tier_limits) AS val
    FROM price_tiers
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        effective_from,
        DENSE_RANK() OVER (PARTITION BY currency ORDER BY val DESC) AS val_dense_rank
    FROM lines
),
scored_price_tiers AS (
    -- Ranking and gap features
    SELECT *, CUME_DIST() OVER (ORDER BY val_dense_rank DESC NULLS LAST) AS val_dense_rank_cume_dist
    FROM scored
),
valid AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        LAST_DAY(effective_from) AS effective_from_month_end,
        val_dense_rank_cume_dist + val_dense_rank AS val_dense_rank_cume_dist_plus_val_dense_rank
    FROM scored_price_tiers
    WHERE effective_from > DATE '2024-02-02'
),
base AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        effective_from,
        val_dense_rank,
        val_dense_rank_cume_dist,
        val_dense_rank_cume_dist + val_dense_rank_cume_dist_plus_val_dense_rank AS val_dense_rank_cume_dist_plus_val_dense_rank_cume_dist_plus_val_dense_rank,
        val_dense_rank - val_dense_rank_cume_dist_plus_val_dense_rank AS val_dense_rank_minus_val_dense_rank_cume_dist_plus_val_dense_rank
    FROM valid
    WHERE effective_from > DATE '2024-03-14'
      AND val_dense_rank_cume_dist BETWEEN 0.39 AND 0.55
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN val_dense_rank_cume_dist >= 0.65 THEN 'LARGE' ELSE 'SMALL' END AS val_dense_rank_cume_dist_tier,
    DATE_TRUNC('WEEK', effective_from) AS week_start,
    DATE_TRUNC('MONTH', effective_from) AS activity_month,
    PERCENTILE_APPROX(val_dense_rank_cume_dist, 0.95) AS val_dense_rank_cume_dist_p95,
    COUNT(DISTINCT id) AS distinct_ids,
    COUNT(DISTINCT id) AS distinct_ids_2
FROM base
GROUP BY CASE WHEN val_dense_rank_cume_dist >= 0.65 THEN 'LARGE' ELSE 'SMALL' END, DATE_TRUNC('WEEK', effective_from), DATE_TRUNC('MONTH', effective_from);
