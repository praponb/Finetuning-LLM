SELECT
    id,
    created_at,
    coupon_codes,
    rec.product_id,
    rec.price,
    rec.promo
FROM carts
LATERAL VIEW OUTER EXPLODE(items) x AS rec
