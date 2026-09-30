SELECT
    id,
    customer_id,
    coupon_codes,
    items,
    attributes,
    ARRAY_CONTAINS(MAP_KEYS(attributes), 'utm_source') AS has_utm_source,
    ARRAY_MAX(TRANSFORM(items, li -> li.price)) AS max_item_price,
    COALESCE(attributes['utm_source'], 'direct') AS utm_source_or_default
FROM carts
WHERE ARRAY_CONTAINS(coupon_codes, 'BUNDLE')
