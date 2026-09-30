WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        created_at,
        item.product_id,
        item.promo
    FROM carts
    LATERAL VIEW EXPLODE(items) t AS item
),
cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, ROUND(product_id, 2) AS product_id_rounded
    FROM lines
    WHERE created_at >= DATE '2025-03-01'
      AND TO_DATE(created_at) < '2025-01-05'
),
cleaned_carts AS (
    -- Keep recent, valid rows
    SELECT
        id,
        created_at,
        product_id,
        promo,
        product_id_rounded,
        CAST(created_at AS DATE) AS created_at_day
    FROM cleaned
    WHERE created_at >= TO_TIMESTAMP('2025-03-01 17:00:00')
      AND promo IS NOT NULL
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        created_at,
        product_id,
        promo,
        product_id_rounded,
        created_at_day,
        ADD_MONTHS(created_at_day, 12) AS created_at_day_plus_months,
        INITCAP(TRIM(promo)) AS promo_title,
        COALESCE(promo, 'NONE') AS promo_clean
    FROM cleaned_carts
    WHERE promo IS NOT NULL
),
with_windows AS (
    -- Running and moving aggregates
    SELECT *, MIN(product_id_rounded) OVER w_grp AS created_at_period_min_product_id_rounded
    FROM filtered
    WHERE product_id BETWEEN 4 AND 6
    WINDOW w_grp AS (PARTITION BY DATE_TRUNC('week', created_at), promo)
),
scored AS (
    -- Running and moving aggregates
    SELECT
        *,
        MAX(created_at_period_min_product_id_rounded) OVER (PARTITION BY YEAR(created_at_day), promo ORDER BY CAST(created_at AS BIGINT) RANGE BETWEEN 7200 PRECEDING AND CURRENT ROW) AS created_at_period_min_product_id_rounded_max_2h,
        AVG(product_id_rounded) OVER (PARTITION BY promo ORDER BY CAST(created_at AS BIGINT) RANGE BETWEEN 3600 PRECEDING AND CURRENT ROW) AS product_id_rounded_avg_1h,
        MAX(product_id_rounded) OVER (PARTITION BY promo ORDER BY CAST(created_at AS BIGINT) RANGE BETWEEN 1800 PRECEDING AND CURRENT ROW) AS product_id_rounded_max_30m,
        DENSE_RANK() OVER w_recent AS created_at_period_min_product_id_rounded_dense_rank
    FROM with_windows
    WINDOW w_recent AS (ORDER BY created_at_period_min_product_id_rounded DESC)
),
filtered_carts AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, CASE WHEN created_at_period_min_product_id_rounded_dense_rank >= 8 THEN 'High' WHEN created_at_period_min_product_id_rounded_dense_rank >= 3 THEN 'Medium' ELSE 'Low' END AS created_at_period_min_product_id_rounded_dense_rank_level
    FROM scored
    WHERE promo IS NOT NULL
      AND MONTH(created_at_day) IN (4, 5, 12)
)
SELECT
    product_id,
    promo_title,
    created_at_period_min_product_id_rounded_max_2h,
    product_id_rounded_avg_1h,
    product_id_rounded_max_30m,
    created_at_period_min_product_id_rounded_dense_rank,
    created_at_period_min_product_id_rounded_dense_rank_level
FROM filtered_carts
ORDER BY created_at_period_min_product_id_rounded_dense_rank;
