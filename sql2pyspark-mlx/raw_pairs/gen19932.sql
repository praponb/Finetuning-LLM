SELECT
    t.id,
    t.customer_id,
    t.coupon_codes,
    pos,
    line.qty,
    line.price,
    line.promo
FROM carts t
LATERAL VIEW POSEXPLODE(t.items) e AS pos, line;
