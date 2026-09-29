WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        created_at,
        line.product_id,
        line.price,
        line.promo
    FROM carts
    LATERAL VIEW EXPLODE(items) li AS line
),
enriched AS (
    -- Ranking and gap features
    SELECT
        *,
        PERCENT_RANK() OVER (PARTITION BY TO_DATE(created_at), promo ORDER BY price DESC) AS price_percent_rank,
        MAX(product_id) OVER (PARTITION BY promo) AS promo_max_product_id,
        AVG(product_id) OVER (ORDER BY CAST(created_at AS BIGINT) RANGE BETWEEN 3600 PRECEDING AND CURRENT ROW) AS product_id_avg_1h
    FROM flattened
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT *, ROUND(price * 100, 2) AS price_adjusted
    FROM enriched
    WHERE created_at >= TIMESTAMP '2025-03-01 12:00:00'
      AND promo IS NOT NULL
),
base AS (
    -- Keep recent, valid rows
    SELECT
        *,
        CAST(promo_max_product_id AS STRING) AS promo_max_product_id_str,
        COALESCE(promo, 'N/A') AS promo_clean
    FROM cleaned
    WHERE promo IS DISTINCT FROM 'SPRING10'
),
scored AS (
    -- Ranking and gap features
    SELECT
        *,
        PERCENT_RANK() OVER (PARTITION BY promo ORDER BY promo_max_product_id DESC) AS promo_max_product_id_percent_rank,
        STDDEV_SAMP(price) OVER (PARTITION BY promo) AS promo_stddev_samp_price,
        CUME_DIST() OVER (PARTITION BY promo ORDER BY product_id DESC NULLS LAST) AS product_id_cume_dist
    FROM base
)
SELECT
    id,
    created_at,
    product_id,
    promo,
    promo_max_product_id,
    product_id_avg_1h,
    price_adjusted,
    promo_clean,
    promo_max_product_id_percent_rank,
    CASE WHEN promo RLIKE '^(VIP|BUNDLE)$' THEN 'GROUP_B' WHEN promo RLIKE '^(SPRING10|WELCOME5)$' THEN 'EXTENDED' ELSE 'GROUP_A' END AS promo_zone
FROM scored
ORDER BY promo_zone DESC NULLS FIRST;
