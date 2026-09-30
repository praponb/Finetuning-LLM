SELECT
    id,
    created_at,
    coupon_codes,
    li_item.product_id,
    li_item.qty,
    li_item.price,
    li_item.promo
FROM (
    SELECT
        id,
        created_at,
        coupon_codes,
        EXPLODE_OUTER(items) AS li_item
    FROM carts
) x;
