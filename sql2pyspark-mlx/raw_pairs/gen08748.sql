WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        effective_from,
        val
    FROM price_tiers
    LATERAL VIEW EXPLODE(discounts) t AS val
),
base AS (
    -- Keep recent, valid rows
    SELECT *, DATEDIFF(CURRENT_DATE(), effective_from) AS days_since_effective_from
    FROM lines
    WHERE val >= 40
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        *,
        PERCENT_RANK() OVER (ORDER BY days_since_effective_from DESC NULLS LAST) AS days_since_effective_from_percent_rank,
        DENSE_RANK() OVER (ORDER BY val ASC NULLS LAST) AS val_dense_rank,
        MIN(days_since_effective_from) OVER (PARTITION BY MONTH(effective_from)) AS effective_from_month_min_days_since_effective_from
    FROM base
),
with_windows_price_tiers AS (
    -- Ranking and gap features
    SELECT
        id,
        val,
        days_since_effective_from,
        days_since_effective_from_percent_rank,
        val_dense_rank,
        effective_from_month_min_days_since_effective_from,
        DENSE_RANK() OVER (ORDER BY effective_from_month_min_days_since_effective_from DESC NULLS FIRST) AS effective_from_month_min_days_since_effective_from_dense_rank,
        DENSE_RANK() OVER (ORDER BY val DESC NULLS FIRST) AS val_dense_rank_2,
        CUME_DIST() OVER w_ordered AS val_cume_dist,
        RANK() OVER w_ordered_2 AS val_dense_rank_rank,
        PERCENT_RANK() OVER (ORDER BY val_dense_rank DESC) AS val_dense_rank_percent_rank
    FROM with_windows
    WHERE days_since_effective_from >= 51
    WINDOW w_ordered AS (ORDER BY val DESC NULLS LAST), w_ordered_2 AS (ORDER BY val_dense_rank ASC)
),
with_windows_price_tiers_2 AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        days_since_effective_from,
        days_since_effective_from_percent_rank,
        val_dense_rank,
        effective_from_month_min_days_since_effective_from_dense_rank,
        PERCENT_RANK() OVER (ORDER BY val_dense_rank_2 DESC) AS val_dense_rank_2_percent_rank,
        PERCENT_RANK() OVER (ORDER BY val_dense_rank_percent_rank DESC NULLS FIRST) AS val_dense_rank_percent_rank_percent_rank,
        DENSE_RANK() OVER (ORDER BY days_since_effective_from_percent_rank DESC) AS days_since_effective_from_percent_rank_dense_rank,
        DENSE_RANK() OVER (ORDER BY val_cume_dist DESC NULLS LAST) AS val_cume_dist_dense_rank,
        PERCENT_RANK() OVER (ORDER BY days_since_effective_from_percent_rank ASC) AS days_since_effective_from_percent_rank_percent_rank
    FROM with_windows_price_tiers
),
with_windows_price_tiers_3 AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        days_since_effective_from,
        val_dense_rank,
        effective_from_month_min_days_since_effective_from_dense_rank,
        val_cume_dist_dense_rank,
        CUME_DIST() OVER (ORDER BY days_since_effective_from_percent_rank_dense_rank ASC) AS days_since_effective_from_percent_rank_dense_rank_cume_dist,
        DENSE_RANK() OVER w_recent AS days_since_effective_from_dense_rank
    FROM with_windows_price_tiers_2
    WINDOW w_recent AS (ORDER BY days_since_effective_from ASC)
)
-- Grouped metrics
SELECT
    CASE WHEN effective_from_month_min_days_since_effective_from_dense_rank >= 7 THEN 'HIGH' ELSE 'LOW' END AS effective_from_month_min_days_since_effective_from_dense_rank_tier,
    CASE WHEN days_since_effective_from_percent_rank_dense_rank_cume_dist >= 0.8 THEN 'LARGE' ELSE 'SMALL' END AS days_since_effective_from_percent_rank_dense_rank_cume_dist_tier,
    CASE WHEN val_dense_rank >= 7 THEN 'LARGE' ELSE 'SMALL' END AS val_dense_rank_tier,
    PERCENTILE_APPROX(days_since_effective_from_percent_rank_dense_rank_cume_dist, ARRAY(0.25, 0.5, 0.75)) AS days_since_effective_from_percent_rank_dense_rank_cume_dist_quartiles,
    PERCENTILE_APPROX(days_since_effective_from_dense_rank, 0.95) AS days_since_effective_from_dense_rank_p95,
    COUNT(*) AS event_count
FROM with_windows_price_tiers_3
GROUP BY CASE WHEN effective_from_month_min_days_since_effective_from_dense_rank >= 7 THEN 'HIGH' ELSE 'LOW' END, CASE WHEN days_since_effective_from_percent_rank_dense_rank_cume_dist >= 0.8 THEN 'LARGE' ELSE 'SMALL' END, CASE WHEN val_dense_rank >= 7 THEN 'LARGE' ELSE 'SMALL' END
ORDER BY days_since_effective_from_percent_rank_dense_rank_cume_dist_tier DESC
