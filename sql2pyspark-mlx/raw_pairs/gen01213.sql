WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        effective_from,
        currency,
        EXPLODE(tier_prices) AS val
    FROM price_tiers
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        RANK() OVER (PARTITION BY currency ORDER BY val) AS val_rank,
        PERCENT_RANK() OVER (PARTITION BY currency ORDER BY val DESC NULLS LAST) AS val_percent_rank,
        PERCENT_RANK() OVER w_grp AS val_percent_rank_2,
        CUME_DIST() OVER w_grp AS val_cume_dist,
        PERCENT_RANK() OVER (PARTITION BY currency ORDER BY val DESC) AS val_percent_rank_3
    FROM flattened
    WINDOW w_grp AS (PARTITION BY currency ORDER BY val NULLS FIRST)
),
scored AS (
    -- Running and moving aggregates
    SELECT
        id,
        effective_from,
        val_rank,
        val_percent_rank_2,
        val_cume_dist,
        DENSE_RANK() OVER (PARTITION BY MONTH(effective_from), currency ORDER BY val_cume_dist DESC NULLS FIRST) AS val_cume_dist_dense_rank
    FROM enriched
    WHERE val_percent_rank_3 <= 0.37
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT
        id,
        effective_from,
        val_rank,
        val_percent_rank_2,
        val_cume_dist,
        DATE_FORMAT(effective_from, 'yyyy-MM-dd') AS effective_from_label,
        val_cume_dist_dense_rank * val_cume_dist AS val_cume_dist_dense_rank_times_val_cume_dist
    FROM scored
    WHERE effective_from > TO_DATE('2024-03-18')
      AND val_percent_rank_2 > 0.77
)
-- Multi-level aggregation
SELECT
    CASE WHEN val_cume_dist >= 0.69 THEN 'LARGE' ELSE 'SMALL' END AS val_cume_dist_tier,
    CASE WHEN val_percent_rank_2 >= 0.74 THEN 'HIGH' ELSE 'LOW' END AS val_percent_rank_2_tier,
    COUNT(*) AS cnt,
    ROUND(AVG(val_cume_dist_dense_rank_times_val_cume_dist), 2) AS avg_val_cume_dist_dense_rank_times_val_cume_dist,
    APPROX_PERCENTILE(val_cume_dist, ARRAY(0.25, 0.5, 0.75)) AS val_cume_dist_quartiles,
    GROUPING(CASE WHEN val_cume_dist >= 0.69 THEN 'LARGE' ELSE 'SMALL' END) AS grouping_level
FROM cleaned
GROUP BY GROUPING SETS ((CASE WHEN val_cume_dist >= 0.69 THEN 'LARGE' ELSE 'SMALL' END, CASE WHEN val_percent_rank_2 >= 0.74 THEN 'HIGH' ELSE 'LOW' END), (CASE WHEN val_cume_dist >= 0.69 THEN 'LARGE' ELSE 'SMALL' END), ())
ORDER BY val_cume_dist_tier DESC;
