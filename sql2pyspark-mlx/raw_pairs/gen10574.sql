SELECT
    c.id,
    c.coupon_codes,
    elem.product_id,
    elem.qty,
    elem.price,
    elem.promo
FROM carts c
LATERAL VIEW OUTER EXPLODE(c.items) e AS elem;
