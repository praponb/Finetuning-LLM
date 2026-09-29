WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        customer_id,
        created_at,
        coupon_codes,
        li_item.product_id,
        li_item.promo
    FROM (
        SELECT
            id,
            customer_id,
            created_at,
            coupon_codes,
            EXPLODE_OUTER(items) AS li_item
        FROM carts
    ) e
),
with_dims AS (
    -- Attach customers attributes
    SELECT
        s.*,
        b.age,
        b.country,
        b.segment
    FROM flattened s
    LEFT JOIN customers b
        ON s.customer_id = b.id
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        COALESCE(promo, 'N/A') AS promo_filled,
        COALESCE(segment, 'OTHER') AS segment_clean
    FROM with_dims
    WHERE created_at >= TIMESTAMP '2025-03-01 22:00:00'
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        created_at,
        coupon_codes,
        product_id,
        promo,
        age,
        country,
        segment,
        promo_filled,
        age + product_id AS age_plus_product_id
    FROM filtered
    WHERE promo IS NOT NULL
)
-- Window metrics over several different partitions
SELECT
    *,
    STDDEV_SAMP(age_plus_product_id) OVER (PARTITION BY country) AS country_stddev_samp_age_plus_product_id,
    PERCENT_RANK() OVER (PARTITION BY country ORDER BY product_id DESC) AS product_id_percent_rank,
    ROUND(age / SUM(age) OVER (PARTITION BY promo), 4) AS age_share
FROM base
WHERE age BETWEEN 35 AND 47
