WITH base AS (
    -- Filter policies
    SELECT
        id,
        holder_id,
        product,
        start_date,
        end_date,
        status,
        LENGTH(TRIM(status)) AS status_len,
        SUBSTRING(TRIM(product), 1, 4) AS product_prefix,
        CASE WHEN premium >= 3544.1 THEN 'A' WHEN premium >= 2033.8 THEN 'B' ELSE 'C' END AS premium_tier
    FROM policies
    WHERE premium >= 1051.9
),
per_group AS (
    -- Aggregate with conditional metrics
    SELECT
        DATE_FORMAT(start_date, 'yyyy-MM') AS start_date_ym,
        premium_tier,
        ARRAY_SORT(COLLECT_SET(product)) AS product_list,
        ROUND(STDDEV(status_len), 2) AS stddev_status_len,
        MIN(status_len) AS min_status_len
    FROM base
    WHERE product_prefix IS NOT NULL
    GROUP BY DATE_FORMAT(start_date, 'yyyy-MM'), premium_tier
),
windowed AS (
    -- Previous / next values per key
    SELECT
        *,
        LEAD(stddev_status_len, 1, 0) OVER (PARTITION BY min_status_len ORDER BY stddev_status_len DESC, start_date_ym, premium_tier) AS next_stddev_status_len,
        MAX(min_status_len) OVER (PARTITION BY min_status_len ORDER BY stddev_status_len DESC, start_date_ym, premium_tier ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS min_status_len_max_2,
        LAST_VALUE(stddev_status_len) IGNORE NULLS OVER (PARTITION BY premium_tier, min_status_len ORDER BY min_status_len ASC, start_date_ym, premium_tier ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_stddev_status_len,
        DENSE_RANK() OVER w_premium_tier_min_status_len_2 AS min_status_len_dense_rank,
        COUNT(*) OVER (PARTITION BY premium_tier) AS rows_in_premium_tier,
        ROUND(min_status_len - AVG(min_status_len) OVER (PARTITION BY premium_tier), 2) AS min_status_len_vs_premium_tier_avg
    FROM per_group
    WINDOW w_premium_tier_min_status_len_2 AS (PARTITION BY premium_tier, min_status_len ORDER BY min_status_len ASC)
),
scored AS (
    -- Derive helper columns
    SELECT *, LENGTH(TRIM(start_date_ym)) AS start_date_ym_len
    FROM windowed
),
sequenced AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        LEAD(min_status_len) OVER (PARTITION BY start_date_ym_len, premium_tier ORDER BY min_status_len_dense_rank DESC, start_date_ym, premium_tier) AS next_min_status_len,
        DENSE_RANK() OVER w_start_date_ym_len_premium_ti_2 AS min_status_len_dense_rank_dense_rank,
        ROUND(min_status_len_dense_rank - AVG(min_status_len_dense_rank) OVER (PARTITION BY min_status_len_dense_rank), 2) AS min_status_len_dense_rank_vs_min_status_len_dens,
        ROW_NUMBER() OVER w_start_date_ym_len_by_min_sta AS rn,
        ROUND(CUME_DIST() OVER (PARTITION BY start_date_ym_len, premium_tier ORDER BY min_status_len_dense_rank DESC), 3) AS min_status_len_dense_rank_cume_dist,
        SUM(next_stddev_status_len) OVER (PARTITION BY min_status_len_dense_rank) AS min_status_len_dense_rank_sum_next_stddev_status
    FROM scored
    WINDOW w_start_date_ym_len_premium_ti_2 AS (PARTITION BY start_date_ym_len, premium_tier ORDER BY min_status_len_dense_rank DESC), w_start_date_ym_len_by_min_sta AS (PARTITION BY start_date_ym_len ORDER BY min_status_len_max_2 DESC, start_date_ym, premium_tier)
),
derived AS (
    -- Label and score rows
    SELECT
        start_date_ym,
        premium_tier,
        product_list,
        min_status_len,
        next_stddev_status_len,
        min_status_len_max_2,
        rows_in_premium_tier,
        min_status_len_vs_premium_tier_avg,
        start_date_ym_len,
        min_status_len_dense_rank_dense_rank,
        rn,
        min_status_len_dense_rank_cume_dist,
        min_status_len_dense_rank_sum_next_stddev_status,
        min_status_len_max_2 + stddev_status_len AS min_status_len_max_2_plus_stddev_status_len,
        ROUND(stddev_status_len * 0.25, 2) AS stddev_status_len_adj
    FROM sequenced
)
-- Native SQL UNPIVOT (wide to long)
SELECT start_date_ym, premium_tier, metric_name, amount
FROM (
    SELECT start_date_ym, premium_tier, min_status_len_vs_premium_tier_avg, min_status_len_max_2_plus_stddev_status_len
    FROM derived
) src
UNPIVOT INCLUDE NULLS (
    amount FOR metric_name IN (min_status_len_vs_premium_tier_avg AS min_status_len_vs_premium_tier_avg_v, min_status_len_max_2_plus_stddev_status_len AS m_min_status_len_max_2_plus_stddev_status_len)
) unp
ORDER BY start_date_ym, premium_tier, metric_name
