WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        customer_id,
        coupon_codes,
        INLINE(items)
    FROM carts
),
flattened AS (
    -- Unnest the array into one row per element
    SELECT
        customer_id,
        product_id,
        price,
        promo,
        tag
    FROM lines
    LATERAL VIEW OUTER EXPLODE(coupon_codes) t AS tag
),
valid AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        CONCAT_WS('-', tag, promo) AS tag_promo_key,
        ROUND(product_id, 2) AS product_id_rounded,
        COALESCE(promo, 'OTHER') AS promo_filled
    FROM flattened
    WHERE tag NOT IN ('SPRING10', 'WELCOME5')
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        customer_id,
        product_id,
        price,
        product_id_rounded,
        CUME_DIST() OVER (ORDER BY product_id_rounded ASC NULLS LAST) AS product_id_rounded_cume_dist,
        RANK() OVER (PARTITION BY promo ORDER BY price DESC NULLS FIRST) AS price_rank,
        COUNT(product_id_rounded) OVER (PARTITION BY customer_id) AS customer_id_count_product_id_rounded
    FROM valid
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        MIN(customer_id_count_product_id_rounded) OVER (PARTITION BY customer_id) AS customer_id_min_customer_id_count_product_id_rounded,
        PERCENT_RANK() OVER (PARTITION BY customer_id ORDER BY price_rank DESC NULLS LAST) AS price_rank_percent_rank,
        ROUND(customer_id_count_product_id_rounded / SUM(customer_id_count_product_id_rounded) OVER (PARTITION BY customer_id), 4) AS customer_id_count_product_id_rounded_share,
        ROUND(product_id / SUM(product_id) OVER (PARTITION BY customer_id), 4) AS product_id_share
    FROM enriched
    WHERE price_rank IS NULL
)
-- Join a dimension and aggregate
SELECT
    d.segment,
    COUNT(*) AS num_rows,
    MAX(a.product_id_rounded_cume_dist) AS max_product_id_rounded_cume_dist,
    SUM(a.price_rank_percent_rank) AS total_price_rank_percent_rank
FROM with_windows a
JOIN customers d ON a.customer_id = d.id
GROUP BY d.segment;
