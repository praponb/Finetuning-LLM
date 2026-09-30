SELECT
    id,
    created_at,
    ship_to,
    ARRAY_MAX(TRANSFORM(items, item -> item.price)) AS max_item_price,
    EXISTS(items, item -> item.price > 100) AS has_premium_item
FROM carts
