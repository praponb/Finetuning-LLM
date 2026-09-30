SELECT
    id,
    customer_id,
    created_at,
    coupon_codes,
    item.product_id,
    item.qty,
    item.price
FROM (
    SELECT
        id,
        customer_id,
        created_at,
        coupon_codes,
        EXPLODE_OUTER(items) AS item
    FROM carts
) e;
