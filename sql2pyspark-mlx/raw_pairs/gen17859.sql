WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        src.id,
        src.customer_id,
        src.coupon_codes,
        pos,
        item.product_id,
        item.qty,
        item.price,
        item.promo
    FROM carts src
    LATERAL VIEW OUTER POSEXPLODE(src.items) t AS pos, item
)
SELECT
    s.customer_id,
    s.coupon_codes,
    s.price,
    (SELECT MIN(o2.amount) FROM orders o2 WHERE o2.customer_id = s.customer_id) AS min_order_amount
FROM exploded s;
