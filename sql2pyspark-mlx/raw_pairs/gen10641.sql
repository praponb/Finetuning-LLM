SELECT
    id,
    created_at,
    coupon_codes,
    pos,
    elem.product_id,
    elem.qty,
    elem.price,
    elem.promo
FROM carts
LATERAL VIEW POSEXPLODE(items) x AS pos, elem;
