WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        created_at,
        coupon_codes,
        k,
        v
    FROM carts
    LATERAL VIEW OUTER EXPLODE(attributes) e AS k, v
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        COALESCE(k, 'Unknown') AS k_filled,
        SIZE(coupon_codes) AS coupon_codes_count,
        COALESCE(k, 'OTHER') AS k_clean
    FROM exploded
    WHERE created_at > DATE '2025-03-01'
      AND HOUR(created_at) BETWEEN 11 AND 18
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        AVG(coupon_codes_count) OVER w_recent AS coupon_codes_count_avg_2h,
        PERCENT_RANK() OVER (PARTITION BY k ORDER BY coupon_codes_count DESC NULLS LAST) AS coupon_codes_count_percent_rank
    FROM valid
    WINDOW w_recent AS (PARTITION BY k ORDER BY CAST(created_at AS BIGINT) RANGE BETWEEN 7200 PRECEDING AND CURRENT ROW)
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        SUM(coupon_codes_count_avg_2h) OVER (PARTITION BY k) AS k_sum_coupon_codes_count_avg_2h,
        MAX(coupon_codes_count) OVER (PARTITION BY MONTH(created_at), k) AS created_at_month_max_coupon_codes_count,
        MAX(coupon_codes_count_percent_rank) OVER w_grp AS coupon_codes_count_percent_rank_max_15m,
        DENSE_RANK() OVER (PARTITION BY k ORDER BY coupon_codes_count_percent_rank DESC NULLS FIRST) AS coupon_codes_count_percent_rank_dense_rank,
        AVG(coupon_codes_count) OVER (PARTITION BY YEAR(created_at) ORDER BY CAST(created_at AS BIGINT) RANGE BETWEEN 7200 PRECEDING AND CURRENT ROW) AS coupon_codes_count_avg_2h_2
    FROM scored
    WHERE coupon_codes_count < 62
    WINDOW w_grp AS (ORDER BY CAST(created_at AS BIGINT) RANGE BETWEEN 900 PRECEDING AND CURRENT ROW)
)
SELECT
    id,
    k,
    k_filled,
    coupon_codes_count,
    k_clean,
    coupon_codes_count_percent_rank,
    k_sum_coupon_codes_count_avg_2h,
    created_at_month_max_coupon_codes_count,
    CASE WHEN k RLIKE '^(weight_kg|device)$' THEN 'EXTENDED' ELSE 'CORE' END AS k_zone
FROM metrics
ORDER BY created_at_month_max_coupon_codes_count, id DESC;
