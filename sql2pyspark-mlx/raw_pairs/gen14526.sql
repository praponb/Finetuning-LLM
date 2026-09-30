WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        base.id,
        line.product_id,
        line.qty,
        line.price,
        line.promo
    FROM carts base
    LATERAL VIEW OUTER EXPLODE(base.items) x AS line
),
with_windows AS (
    -- Ranking and gap features
    SELECT *, COUNT(product_id) OVER (PARTITION BY promo) AS promo_count_product_id
    FROM lines
),
base AS (
    -- Keep recent, valid rows
    SELECT
        product_id,
        qty,
        price,
        promo,
        promo_count_product_id,
        qty - promo_count_product_id AS qty_minus_promo_count_product_id
    FROM with_windows
    WHERE promo IS NOT NULL
      AND promo IS DISTINCT FROM 'BUNDLE'
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        product_id,
        price,
        promo,
        qty_minus_promo_count_product_id,
        CUME_DIST() OVER (ORDER BY qty DESC) AS qty_cume_dist,
        ROUND(product_id / SUM(product_id) OVER (PARTITION BY promo), 4) AS product_id_share,
        CUME_DIST() OVER (PARTITION BY promo ORDER BY qty) AS qty_cume_dist_2
    FROM base
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        price,
        promo,
        qty_cume_dist,
        product_id_share,
        qty_cume_dist_2,
        ROUND(product_id_share / product_id, 2) AS product_id_share_per_product_id,
        UPPER(promo) AS promo_upper,
        COALESCE(promo, 'OTHER') AS promo_clean
    FROM metrics
    WHERE promo IS NOT NULL
)
SELECT
    price,
    qty_cume_dist,
    product_id_share,
    qty_cume_dist_2,
    product_id_share_per_product_id,
    promo_upper,
    promo_clean
FROM valid;
