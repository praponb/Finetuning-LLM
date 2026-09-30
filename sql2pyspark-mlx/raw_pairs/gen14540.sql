WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT id, val
    FROM price_tiers
    LATERAL VIEW EXPLODE(tier_prices) x AS val
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        *,
        DENSE_RANK() OVER w_recent val_dense_rank,
        RANK() OVER w_recent val_rank,
        PERCENT_RANK() OVER w_ordered val_percent_rank,
        CUME_DIST() OVER w_ordered val_cume_dist
    FROM flattened
    WHERE val BETWEEN 53 AND 75
    WINDOW w_recent AS (ORDER BY val DESC NULLS LAST), w_ordered AS (ORDER BY val DESC NULLS FIRST)
),
base AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        GREATEST(val_dense_rank, val) greatest_val_dense_rank_val,
        ROUND(val_percent_rank * 0.85, 2) val_percent_rank_scaled
    FROM enriched
    WHERE val_cume_dist >= 0.51
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, CASE WHEN val_percent_rank >= 0.66 THEN 'High' WHEN val_percent_rank >= 0.12 THEN 'Medium' ELSE 'Low' END val_percent_rank_level
    FROM base
    WHERE greatest_val_dense_rank_val BETWEEN 23 AND 71
),
enriched_price_tiers AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        val,
        val_rank,
        val_percent_rank,
        val_cume_dist,
        greatest_val_dense_rank_val,
        val_percent_rank_level,
        RANK() OVER w_running val_percent_rank_rank,
        PERCENT_RANK() OVER (ORDER BY val_rank DESC NULLS LAST) val_rank_percent_rank,
        DENSE_RANK() OVER w_recent val_rank_dense_rank
    FROM filtered
    WINDOW w_running AS (ORDER BY val_percent_rank DESC), w_recent AS (ORDER BY val_rank)
),
metrics AS (
    -- Running and moving aggregates
    SELECT *, PERCENT_RANK() OVER (ORDER BY val_percent_rank_rank DESC NULLS FIRST) val_percent_rank_rank_percent_rank
    FROM enriched_price_tiers
)
SELECT
    id,
    val,
    val_rank,
    val_percent_rank,
    val_cume_dist,
    greatest_val_dense_rank_val,
    val_percent_rank_level,
    val_percent_rank_rank,
    val_rank_dense_rank
FROM metrics
ORDER BY id DESC
