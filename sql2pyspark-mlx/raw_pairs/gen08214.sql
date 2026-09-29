SELECT
    id,
    created_at,
    pos + 1 AS item_no,
    elem.product_id,
    elem.qty,
    elem.price,
    elem.promo
FROM carts
LATERAL VIEW OUTER POSEXPLODE(items) t AS pos, elem;
