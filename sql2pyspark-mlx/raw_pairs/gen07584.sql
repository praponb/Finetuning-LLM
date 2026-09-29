WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        customer_id,
        created_at,
        element
    FROM carts
    LATERAL VIEW OUTER EXPLODE(coupon_codes) t AS element
),
enriched AS (
    -- Attach customers attributes
    SELECT
        e.customer_id,
        e.created_at,
        b.country
    FROM exploded e
    LEFT JOIN customers b
        ON e.customer_id = b.id
),
valid AS (
    -- Keep recent, valid rows
    SELECT *, WEEKOFYEAR(created_at) AS created_at_weekofyear
    FROM enriched
    WHERE created_at >= TIMESTAMP '2025-03-02 03:00:00'
      AND customer_id IS NOT NULL
),
enriched_carts AS (
    -- Window metrics over several different partitions
    SELECT
        customer_id,
        created_at,
        SUM(created_at_weekofyear) OVER w_running AS created_at_weekofyear_sum_1h,
        SUM(created_at_weekofyear) OVER (PARTITION BY country, customer_id ORDER BY CAST(created_at AS BIGINT) RANGE BETWEEN 7200 PRECEDING AND CURRENT ROW) AS created_at_weekofyear_sum_2h,
        COUNT(created_at_weekofyear) OVER (PARTITION BY country ORDER BY CAST(created_at AS BIGINT) RANGE BETWEEN 3600 PRECEDING AND CURRENT ROW) AS created_at_weekofyear_count_1h,
        MIN(created_at_weekofyear) OVER (PARTITION BY YEAR(created_at)) AS created_at_year_min_created_at_weekofyear,
        RANK() OVER (PARTITION BY YEAR(created_at), customer_id ORDER BY created_at_weekofyear) AS created_at_weekofyear_rank
    FROM valid
    WINDOW w_running AS (ORDER BY CAST(created_at AS BIGINT) RANGE BETWEEN 3600 PRECEDING AND CURRENT ROW)
),
with_windows AS (
    -- Running and moving aggregates
    SELECT *, MAX(created_at_weekofyear_count_1h) OVER w_recent AS created_at_weekofyear_count_1h_max_30m
    FROM enriched_carts
    WINDOW w_recent AS (ORDER BY CAST(created_at AS BIGINT) RANGE BETWEEN 1800 PRECEDING AND CURRENT ROW)
)
SELECT
    created_at,
    created_at_weekofyear_sum_1h,
    created_at_weekofyear_count_1h,
    created_at_year_min_created_at_weekofyear,
    created_at_weekofyear_rank
FROM with_windows
ORDER BY created_at_weekofyear_rank NULLS LAST
