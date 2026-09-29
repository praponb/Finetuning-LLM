SELECT
    id,
    created_at,
    items,
    attributes,
    EXISTS(items, x -> x.price > 100) AS has_premium_item,
    attributes['gift'] AS gift
FROM carts;
