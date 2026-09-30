WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        created_at,
        coupon_codes,
        elem.product_id,
        elem.qty,
        elem.price,
        elem.promo
    FROM (
        SELECT
            id,
            created_at,
            coupon_codes,
            EXPLODE_OUTER(items) AS elem
        FROM carts
    ) li
),
lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        product_id,
        price,
        element
    FROM flattened
    LATERAL VIEW EXPLODE(coupon_codes) e AS element
),
base AS (
    -- Keep recent, valid rows
    SELECT
        *,
        LENGTH(element) AS element_length,
        ROUND(price, 1) AS price_rounded
    FROM lines
    WHERE element IS NOT NULL
      AND element IS DISTINCT FROM 'VIP'
),
enriched AS (
    -- Ranking and gap features
    SELECT
        *,
        DENSE_RANK() OVER w_running AS price_rounded_dense_rank,
        DENSE_RANK() OVER (PARTITION BY element ORDER BY price NULLS LAST) AS price_dense_rank,
        RANK() OVER w_grp AS product_id_rank,
        CUME_DIST() OVER w_ordered AS element_length_cume_dist
    FROM base
    WINDOW w_running AS (ORDER BY price_rounded DESC NULLS LAST), w_grp AS (ORDER BY product_id DESC), w_ordered AS (ORDER BY element_length DESC NULLS FIRST)
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        product_id,
        element_length,
        price_dense_rank,
        product_id_rank,
        element_length_cume_dist,
        COUNT(product_id) OVER (PARTITION BY element) AS element_count_product_id,
        RANK() OVER (PARTITION BY element ORDER BY price_rounded_dense_rank DESC) AS price_rounded_dense_rank_rank
    FROM enriched
)
SELECT
    price_dense_rank,
    product_id_rank,
    element_count_product_id
FROM metrics
ORDER BY element_count_product_id NULLS LAST
