WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        effective_from,
        val
    FROM price_tiers
    LATERAL VIEW OUTER EXPLODE(tier_prices) e AS val
),
filtered AS (
    -- Keep recent, valid rows
    SELECT
        *,
        ROUND(val * 1.15, 2) AS adjusted_val,
        DATE_ADD(effective_from, 7) AS effective_from_plus_7d
    FROM lines
    WHERE effective_from >= '2025-06-01'
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        effective_from,
        adjusted_val,
        CUME_DIST() OVER (ORDER BY val NULLS FIRST) AS val_cume_dist,
        CUME_DIST() OVER (ORDER BY val DESC NULLS LAST) AS val_cume_dist_2
    FROM filtered
    WHERE val IS NOT NULL
),
metrics AS (
    -- Ranking and gap features
    SELECT
        effective_from,
        adjusted_val,
        PERCENT_RANK() OVER (ORDER BY val_cume_dist_2 DESC NULLS FIRST) AS val_cume_dist_2_percent_rank,
        DENSE_RANK() OVER (ORDER BY val_cume_dist DESC) AS val_cume_dist_dense_rank,
        RANK() OVER (ORDER BY val_cume_dist_2 DESC) AS val_cume_dist_2_rank
    FROM scored
)
SELECT
    effective_from,
    adjusted_val,
    val_cume_dist_2_rank
FROM metrics
ORDER BY val_cume_dist_2_rank DESC, adjusted_val;
