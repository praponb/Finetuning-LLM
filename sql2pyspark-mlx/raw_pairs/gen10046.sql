SELECT
    id,
    created_at,
    entry.product_id,
    entry.qty,
    entry.price,
    entry.promo
FROM carts
LATERAL VIEW OUTER EXPLODE(items) t AS entry;
