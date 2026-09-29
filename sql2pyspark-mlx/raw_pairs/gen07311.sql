WITH flattened AS (
    SELECT
        id,
        customer_id,
        created_at,
        coupon_codes,
        INLINE(items)
    FROM carts
)
SELECT
    id,
    created_at,
    product_id,
    price,
    PERCENT_RANK() OVER (PARTITION BY promo ORDER BY product_id NULLS LAST) product_id_percent_rank,
    COUNT(product_id) OVER (PARTITION BY customer_id ORDER BY CAST(created_at AS BIGINT) RANGE BETWEEN 3600 PRECEDING AND CURRENT ROW) product_id_count_1h,
    COUNT(qty) OVER (ORDER BY CAST(created_at AS BIGINT) RANGE BETWEEN 900 PRECEDING AND CURRENT ROW) qty_count_15m
FROM flattened;
