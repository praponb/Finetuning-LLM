SELECT
    id,
    created_at,
    attributes,
    ARRAY_MAX(TRANSFORM(items, li -> li.price)) AS max_item_price,
    FORALL(items, li -> li.qty <= 5) AS all_small_qty,
    ARRAY_CONTAINS(coupon_codes, 'VIP') AS used_vip,
    COALESCE(attributes['device'], 'none') AS device_or_default
FROM carts
