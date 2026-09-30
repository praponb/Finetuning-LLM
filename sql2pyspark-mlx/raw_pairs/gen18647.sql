WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        c.id,
        c.created_at,
        c.coupon_codes,
        line.product_id,
        line.qty,
        line.price,
        line.promo
    FROM carts c
    LATERAL VIEW EXPLODE(c.items) li AS line
),
flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        created_at,
        qty,
        price,
        promo,
        EXPLODE_OUTER(coupon_codes) AS code
    FROM lines
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        created_at,
        MAX(price) OVER (PARTITION BY promo ORDER BY CAST(created_at AS BIGINT) RANGE BETWEEN 7200 PRECEDING AND CURRENT ROW) AS price_max_2h
    FROM flattened
    WHERE created_at < '2025-07-01'
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        FLOOR(price_max_2h) AS price_max_2h_whole,
        ROUND(price_max_2h * 1.07, 2) AS adjusted_price_max_2h
    FROM with_windows
    WHERE created_at >= TIMESTAMP '2025-03-01 20:00:00'
),
with_windows_carts AS (
    -- Running and moving aggregates
    SELECT
        id,
        created_at,
        price_max_2h,
        price_max_2h_whole,
        adjusted_price_max_2h,
        RANK() OVER (ORDER BY price_max_2h_whole DESC NULLS LAST) AS price_max_2h_whole_rank,
        MAX(price_max_2h) OVER (ORDER BY CAST(created_at AS BIGINT) RANGE BETWEEN 900 PRECEDING AND CURRENT ROW) AS price_max_2h_max_15m,
        SUM(adjusted_price_max_2h) OVER w_ordered AS adjusted_price_max_2h_sum_1h
    FROM filtered
    WINDOW w_ordered AS (PARTITION BY DATE_TRUNC('month', created_at) ORDER BY CAST(created_at AS BIGINT) RANGE BETWEEN 3600 PRECEDING AND CURRENT ROW)
),
enriched AS (
    -- Ranking and gap features
    SELECT
        id,
        price_max_2h_whole,
        adjusted_price_max_2h,
        price_max_2h_max_15m,
        adjusted_price_max_2h_sum_1h,
        CUME_DIST() OVER (ORDER BY adjusted_price_max_2h DESC NULLS LAST) AS adjusted_price_max_2h_cume_dist,
        RANK() OVER (PARTITION BY TO_DATE(created_at) ORDER BY adjusted_price_max_2h NULLS FIRST) AS adjusted_price_max_2h_rank,
        DENSE_RANK() OVER (ORDER BY adjusted_price_max_2h DESC NULLS FIRST) AS adjusted_price_max_2h_dense_rank,
        DENSE_RANK() OVER (PARTITION BY MONTH(created_at) ORDER BY adjusted_price_max_2h_sum_1h DESC NULLS FIRST) AS adjusted_price_max_2h_sum_1h_dense_rank,
        CUME_DIST() OVER (ORDER BY price_max_2h NULLS FIRST) AS price_max_2h_cume_dist
    FROM with_windows_carts
),
filtered_carts AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        CAST(adjusted_price_max_2h AS INT) AS adjusted_price_max_2h_int,
        CAST(price_max_2h_cume_dist AS INT) AS price_max_2h_cume_dist_int
    FROM enriched
    WHERE adjusted_price_max_2h_cume_dist IS NOT NULL
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN adjusted_price_max_2h_int >= 75 THEN 'PREMIUM' ELSE 'STANDARD' END AS adjusted_price_max_2h_int_tier,
    COUNT(*) AS num_rows,
    APPROX_PERCENTILE(adjusted_price_max_2h_rank, ARRAY(0.25, 0.5, 0.75)) AS adjusted_price_max_2h_rank_quartiles,
    MAX(adjusted_price_max_2h_sum_1h_dense_rank) AS max_adjusted_price_max_2h_sum_1h_dense_rank
FROM filtered_carts
GROUP BY CASE WHEN adjusted_price_max_2h_int >= 75 THEN 'PREMIUM' ELSE 'STANDARD' END
HAVING max_adjusted_price_max_2h_sum_1h_dense_rank > 0
ORDER BY adjusted_price_max_2h_int_tier DESC NULLS LAST;
