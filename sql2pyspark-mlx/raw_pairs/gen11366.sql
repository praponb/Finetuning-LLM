WITH flattened AS (
    SELECT
        id,
        coupon_codes,
        INLINE(items)
    FROM carts
),
flattened_carts AS (
    SELECT
        id,
        product_id,
        qty,
        promo,
        EXPLODE_OUTER(coupon_codes) AS code
    FROM flattened
),
metrics AS (
    SELECT
        *,
        STDDEV_SAMP(qty) OVER (PARTITION BY promo) AS promo_stddev_samp_qty,
        ROUND(product_id / SUM(product_id) OVER (PARTITION BY code), 4) AS product_id_share
    FROM flattened_carts
    WHERE product_id != 8
),
scored AS (
    SELECT
        *,
        PERCENT_RANK() OVER (PARTITION BY promo ORDER BY promo_stddev_samp_qty DESC) AS promo_stddev_samp_qty_percent_rank,
        PERCENT_RANK() OVER (PARTITION BY code ORDER BY product_id NULLS LAST) AS product_id_percent_rank,
        AVG(product_id) OVER (PARTITION BY code) AS code_avg_product_id
    FROM metrics
),
valid AS (
    SELECT
        id,
        product_id,
        qty,
        code,
        promo_stddev_samp_qty,
        product_id_share,
        promo_stddev_samp_qty_percent_rank,
        product_id_percent_rank,
        code_avg_product_id,
        COALESCE(promo, 'N/A') AS promo_filled,
        COALESCE(code, 'DIRECT') AS resolved_code
    FROM scored
    WHERE code IS DISTINCT FROM 'BUNDLE'
      AND product_id < 9
),
with_windows AS (
    SELECT
        *,
        MAX(product_id_percent_rank) OVER (PARTITION BY code) AS code_max_product_id_percent_rank,
        ROUND(qty / SUM(qty) OVER (PARTITION BY code), 4) AS qty_share,
        CUME_DIST() OVER (PARTITION BY code ORDER BY product_id_percent_rank DESC NULLS LAST) AS product_id_percent_rank_cume_dist
    FROM valid
),
report AS (
    SELECT
        id,
        product_id,
        code,
        promo_stddev_samp_qty,
        product_id_share,
        product_id_percent_rank,
        code_avg_product_id,
        promo_filled,
        resolved_code,
        CASE WHEN code RLIKE '^(WELCOME5|BUNDLE)$' THEN 'OTHER_ZONE' WHEN code RLIKE '^(SPRING10|VIP)$' THEN 'PRIORITY' ELSE 'EXTENDED' END AS code_zone
    FROM with_windows
    ORDER BY product_id_share DESC NULLS LAST
)
SELECT
    id,
    product_id,
    code,
    promo_filled,
    resolved_code,
    code_zone,
    CASE WHEN code_zone RLIKE '^(PRIORITY|OTHER_ZONE)$' THEN 'GROUP_A' ELSE 'EXTENDED' END AS code_zone_zone
FROM report;
