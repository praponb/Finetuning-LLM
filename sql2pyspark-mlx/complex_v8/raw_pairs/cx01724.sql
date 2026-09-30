WITH valid AS (
    -- Filter sites
    SELECT
        id,
        site_name,
        zone,
        tariff,
        installed_kw,
        commissioned_on,
        CASE WHEN zone IN ('Z2', 'Z3') THEN 'group_a' WHEN zone = 'Z4' THEN 'group_b' ELSE 'other' END AS zone_group,
        INITCAP(TRIM(zone)) AS zone_clean,
        COALESCE(installed_kw, 0.0) AS installed_kw_filled
    FROM sites
    WHERE installed_kw < 284.0
      AND tariff NOT IN ('EV', 'TOU')
),
rollup_base AS (
    -- Group-level totals
    SELECT
        CASE WHEN installed_kw_filled >= 326.8 THEN 'large' WHEN installed_kw_filled >= 128.8 THEN 'medium' ELSE 'small' END AS installed_kw_filled_tier,
        ROUND(100 * COUNT(CASE WHEN tariff IS NOT NULL THEN 1 END) / COUNT(*), 1) AS pct_tariff_is_not_null,
        COUNT(*) AS n_rows,
        COUNT(DISTINCT zone) AS distinct_zones,
        ARRAY_SORT(COLLECT_SET(zone)) AS zone_list
    FROM valid
    WHERE tariff IN ('FLAT', 'EV')
    GROUP BY CASE WHEN installed_kw_filled >= 326.8 THEN 'large' WHEN installed_kw_filled >= 128.8 THEN 'medium' ELSE 'small' END
),
sequenced AS (
    -- Previous / next values per key
    SELECT
        installed_kw_filled_tier,
        n_rows,
        distinct_zones,
        LAG(pct_tariff_is_not_null) OVER (ORDER BY n_rows, installed_kw_filled_tier) AS prev_pct_tariff_is_not_null,
        ROUND(AVG(n_rows) OVER (PARTITION BY distinct_zones, installed_kw_filled_tier), 2) AS distinct_zones_installed_kw_filled_tier_avg_n_ro,
        ROUND(pct_tariff_is_not_null / NULLIF(SUM(pct_tariff_is_not_null) OVER (PARTITION BY distinct_zones, installed_kw_filled_tier), 0), 4) AS pct_tariff_is_not_null_share_of_distinct_zones_i
    FROM rollup_base
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        RANK() OVER (PARTITION BY n_rows, installed_kw_filled_tier ORDER BY distinct_zones_installed_kw_filled_tier_avg_n_ro) AS distinct_zones_installed_kw_filled_tier_avg_n_ro_2,
        MIN(distinct_zones) OVER (PARTITION BY n_rows, distinct_zones) AS n_rows_distinct_zones_min_distinct_zones,
        MAX(distinct_zones) OVER (PARTITION BY n_rows, distinct_zones) AS n_rows_distinct_zones_max_distinct_zones,
        ROUND(distinct_zones - AVG(distinct_zones) OVER (PARTITION BY n_rows, distinct_zones), 2) AS distinct_zones_vs_n_rows_distinct_zones_avg
    FROM sequenced
),
sequenced_2 AS (
    -- Rank and compare rows within groups
    SELECT
        installed_kw_filled_tier,
        prev_pct_tariff_is_not_null,
        distinct_zones_installed_kw_filled_tier_avg_n_ro,
        distinct_zones_vs_n_rows_distinct_zones_avg,
        ROUND(AVG(distinct_zones) OVER (PARTITION BY installed_kw_filled_tier, distinct_zones), 2) AS installed_kw_filled_tier_distinct_zones_avg_dist,
        MAX(distinct_zones) OVER (PARTITION BY distinct_zones ORDER BY distinct_zones DESC, installed_kw_filled_tier ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS distinct_zones_max_7,
        DENSE_RANK() OVER w_distinct_zones_by_distinct_z_2 AS distinct_zones_dense_rank
    FROM metrics
    WINDOW w_distinct_zones_by_distinct_z_2 AS (PARTITION BY distinct_zones ORDER BY distinct_zones DESC)
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        installed_kw_filled_tier,
        prev_pct_tariff_is_not_null,
        distinct_zones_installed_kw_filled_tier_avg_n_ro,
        distinct_zones_vs_n_rows_distinct_zones_avg,
        installed_kw_filled_tier_distinct_zones_avg_dist,
        distinct_zones_max_7,
        distinct_zones_dense_rank,
        ROUND(CUME_DIST() OVER (PARTITION BY distinct_zones_dense_rank ORDER BY installed_kw_filled_tier_distinct_zones_avg_dist), 3) AS installed_kw_filled_tier_distinct_zones_avg_dist_2,
        ROUND(AVG(prev_pct_tariff_is_not_null) OVER w_installed_kw_filled_tier, 2) AS installed_kw_filled_tier_avg_prev_pct_tariff_is_
    FROM sequenced_2
    WINDOW w_installed_kw_filled_tier AS (PARTITION BY installed_kw_filled_tier)
),
scored AS (
    -- Add calculated columns
    SELECT *, ABS(distinct_zones_max_7 - 16) AS distinct_zones_max_7_distance
    FROM windowed
)
SELECT
    installed_kw_filled_tier,
    prev_pct_tariff_is_not_null,
    distinct_zones_installed_kw_filled_tier_avg_n_ro,
    distinct_zones_vs_n_rows_distinct_zones_avg,
    distinct_zones_dense_rank,
    distinct_zones_max_7_distance,
    CONCAT(installed_kw_filled_tier, '#', CAST(distinct_zones_max_7 AS STRING)) AS installed_kw_filled_tier_ref
FROM scored
ORDER BY distinct_zones_installed_kw_filled_tier_avg_n_ro DESC, prev_pct_tariff_is_not_null DESC
