SELECT
    id,
    created_at,
    coupon_codes,
    items,
    SIZE(attributes) AS num_attributes,
    COALESCE(attributes['gift'], 'none') AS gift_or_default,
    TRANSFORM(items, x -> x.product_id) AS product_ids,
    ship_to.country AS ship_country
FROM carts
WHERE created_at >= '2024-04-01'
