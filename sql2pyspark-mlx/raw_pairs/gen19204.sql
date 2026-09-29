WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        coupon_codes,
        li_item.product_id,
        li_item.qty,
        li_item.price
    FROM carts
    LATERAL VIEW OUTER EXPLODE(items) li AS li_item
),
filtered AS (
    -- Keep recent, valid rows
    SELECT
        coupon_codes,
        qty,
        ARRAY_CONTAINS(coupon_codes, 'BUNDLE') AS is_bundle,
        qty - price AS qty_minus_price
    FROM flattened
    WHERE product_id BETWEEN 4 AND 8
),
valid AS (
    -- Keep recent, valid rows
    SELECT *, CAST(is_bundle AS INT) AS is_bundle_flag
    FROM filtered
    WHERE qty >= 4
),
scored AS (
    -- Running and moving aggregates
    SELECT
        coupon_codes,
        is_bundle,
        qty_minus_price,
        is_bundle_flag,
        PERCENT_RANK() OVER (ORDER BY qty DESC NULLS FIRST) AS qty_percent_rank,
        CUME_DIST() OVER (ORDER BY qty_minus_price DESC) AS qty_minus_price_cume_dist
    FROM valid
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        coupon_codes,
        qty_percent_rank,
        qty_minus_price_cume_dist,
        PERCENT_RANK() OVER (ORDER BY qty_minus_price_cume_dist ASC NULLS LAST) AS qty_minus_price_cume_dist_percent_rank,
        DENSE_RANK() OVER (ORDER BY qty_minus_price_cume_dist DESC NULLS LAST) AS qty_minus_price_cume_dist_dense_rank,
        RANK() OVER (ORDER BY qty_minus_price ASC NULLS LAST) AS qty_minus_price_rank
    FROM scored
    WHERE qty_minus_price > 36
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        *,
        PERCENT_RANK() OVER (ORDER BY qty_minus_price_cume_dist_dense_rank DESC NULLS FIRST) AS qty_minus_price_cume_dist_dense_rank_percent_rank,
        CUME_DIST() OVER (ORDER BY qty_minus_price_rank DESC) AS qty_minus_price_rank_cume_dist,
        PERCENT_RANK() OVER (ORDER BY qty_minus_price_cume_dist_percent_rank DESC NULLS FIRST) AS qty_minus_price_cume_dist_percent_rank_percent_rank,
        RANK() OVER (ORDER BY qty_minus_price_cume_dist ASC NULLS FIRST) AS qty_minus_price_cume_dist_rank
    FROM metrics
)
SELECT
    qty_minus_price_cume_dist,
    qty_minus_price_rank,
    qty_minus_price_cume_dist_dense_rank_percent_rank,
    qty_minus_price_cume_dist_percent_rank_percent_rank,
    qty_minus_price_cume_dist_rank
FROM enriched
ORDER BY qty_minus_price_rank ASC
