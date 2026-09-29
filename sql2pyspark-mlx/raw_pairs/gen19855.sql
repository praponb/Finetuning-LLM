SELECT
    base.id,
    base.customer_id,
    base.created_at,
    base.coupon_codes,
    elem.qty,
    elem.price
FROM carts base
LATERAL VIEW OUTER EXPLODE(base.items) x AS elem
