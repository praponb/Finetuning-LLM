WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        src.id,
        src.customer_id,
        src.created_at,
        src.coupon_codes,
        pos + 1 AS line_no,
        line.qty,
        line.price,
        line.promo
    FROM carts src
    LATERAL VIEW OUTER POSEXPLODE(src.items) e AS pos, line
),
with_windows AS (
    -- Ranking and gap features
    SELECT *, DENSE_RANK() OVER (PARTITION BY promo ORDER BY price DESC NULLS FIRST) AS price_dense_rank
    FROM flattened
),
valid AS (
    -- Basic cleansing and defaults
    SELECT *, SORT_ARRAY(coupon_codes) AS sorted_coupon_codes
    FROM with_windows
    WHERE created_at > TIMESTAMP '2025-03-01 19:00:00'
      AND promo IS DISTINCT FROM 'WELCOME5'
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        customer_id,
        created_at,
        coupon_codes,
        line_no,
        price,
        promo,
        price_dense_rank,
        sorted_coupon_codes,
        SORT_ARRAY(coupon_codes) AS sorted_coupon_codes_2,
        price / NULLIF(line_no, 0) AS price_per_line_no
    FROM valid
    WHERE promo IS NOT DISTINCT FROM 'BUNDLE'
      AND created_at > DATE '2025-03-01'
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        created_at,
        coupon_codes,
        price,
        price_dense_rank,
        sorted_coupon_codes,
        sorted_coupon_codes_2,
        price_per_line_no,
        COUNT(line_no) OVER (PARTITION BY promo ORDER BY CAST(created_at AS BIGINT) RANGE BETWEEN 7200 PRECEDING AND CURRENT ROW) AS line_no_count_2h,
        MAX(price_per_line_no) OVER (PARTITION BY promo ORDER BY CAST(created_at AS BIGINT) RANGE BETWEEN 1800 PRECEDING AND CURRENT ROW) AS price_per_line_no_max_30m,
        DENSE_RANK() OVER (PARTITION BY customer_id, promo ORDER BY price DESC NULLS LAST) AS price_dense_rank_2,
        SUM(line_no) OVER (PARTITION BY DATE_TRUNC('MONTH', created_at), promo ORDER BY CAST(created_at AS BIGINT) RANGE BETWEEN 1800 PRECEDING AND CURRENT ROW) AS line_no_sum_30m,
        MAX(price_dense_rank) OVER (PARTITION BY promo ORDER BY CAST(created_at AS BIGINT) RANGE BETWEEN 3600 PRECEDING AND CURRENT ROW) AS price_dense_rank_max_1h
    FROM filtered
)
SELECT
    id,
    created_at,
    price,
    price_dense_rank,
    sorted_coupon_codes,
    sorted_coupon_codes_2,
    price_per_line_no,
    price_per_line_no_max_30m,
    price_dense_rank_2
FROM enriched
ORDER BY price_dense_rank_2 DESC, price_dense_rank DESC;
