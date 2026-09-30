WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        customer_id,
        created_at,
        coupon_codes,
        k,
        value
    FROM carts
    LATERAL VIEW OUTER EXPLODE(attributes) e AS k, value
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT *, CASE WHEN k = 'device' THEN 1 ELSE 0 END AS is_device
    FROM flattened
    WHERE created_at >= '2025-07-01'
),
scored AS (
    -- Running and moving aggregates
    SELECT
        id,
        created_at,
        coupon_codes,
        value,
        is_device,
        PERCENT_RANK() OVER (PARTITION BY customer_id ORDER BY is_device DESC NULLS FIRST) AS is_device_percent_rank
    FROM cleaned
    WHERE created_at < '2023-05-02'
),
scored_carts AS (
    -- Ranking and gap features
    SELECT
        id,
        coupon_codes,
        value,
        SUM(is_device_percent_rank) OVER (ORDER BY CAST(created_at AS BIGINT) RANGE BETWEEN 7200 PRECEDING AND CURRENT ROW) AS is_device_percent_rank_sum_2h,
        COUNT(is_device_percent_rank) OVER (PARTITION BY YEAR(created_at) ORDER BY CAST(created_at AS BIGINT) RANGE BETWEEN 7200 PRECEDING AND CURRENT ROW) AS is_device_percent_rank_count_2h
    FROM scored
),
filtered AS (
    -- Keep recent, valid rows
    SELECT
        id,
        coupon_codes,
        value,
        is_device_percent_rank_sum_2h,
        is_device_percent_rank_count_2h,
        SIZE(coupon_codes) AS coupon_codes_count,
        SIZE(coupon_codes) AS coupon_codes_count_2
    FROM scored_carts
    WHERE is_device_percent_rank_count_2h <> 12
),
scored_carts_2 AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        DENSE_RANK() OVER (ORDER BY coupon_codes_count DESC NULLS FIRST) AS coupon_codes_count_dense_rank,
        DENSE_RANK() OVER (ORDER BY coupon_codes_count_2 NULLS FIRST) AS coupon_codes_count_2_dense_rank,
        PERCENT_RANK() OVER w_ordered AS coupon_codes_count_percent_rank
    FROM filtered
    WINDOW w_ordered AS (ORDER BY coupon_codes_count DESC NULLS LAST)
),
scored_carts_3 AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        coupon_codes,
        coupon_codes_count,
        coupon_codes_count_2,
        coupon_codes_count_dense_rank,
        coupon_codes_count_2_dense_rank,
        coupon_codes_count_percent_rank,
        PERCENT_RANK() OVER (ORDER BY coupon_codes_count_percent_rank DESC NULLS FIRST) AS coupon_codes_count_percent_rank_percent_rank,
        RANK() OVER (ORDER BY coupon_codes_count_2_dense_rank) AS coupon_codes_count_2_dense_rank_rank,
        DENSE_RANK() OVER (ORDER BY is_device_percent_rank_count_2h NULLS LAST) AS is_device_percent_rank_count_2h_dense_rank
    FROM scored_carts_2
)
-- Grouped metrics
SELECT
    CASE WHEN coupon_codes_count >= 90 THEN 'LARGE' ELSE 'SMALL' END AS coupon_codes_count_tier,
    CASE WHEN coupon_codes_count_percent_rank >= 0.74 THEN 'HIGH' ELSE 'LOW' END AS coupon_codes_count_percent_rank_tier,
    COUNT(*) AS num_rows,
    COUNT(DISTINCT id) AS distinct_ids
FROM scored_carts_3
GROUP BY CASE WHEN coupon_codes_count >= 90 THEN 'LARGE' ELSE 'SMALL' END, CASE WHEN coupon_codes_count_percent_rank >= 0.74 THEN 'HIGH' ELSE 'LOW' END
ORDER BY distinct_ids DESC NULLS LAST
