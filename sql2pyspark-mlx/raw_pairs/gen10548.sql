SELECT
    id,
    customer_id,
    created_at,
    coupon_codes,
    COALESCE(attributes['utm_source'], 'none') AS utm_source_or_default,
    TRANSFORM(items, li -> li.product_id) AS product_ids,
    COALESCE(attributes['device'], 'direct') AS device_or_default
FROM carts
WHERE ARRAY_CONTAINS(coupon_codes, 'VIP')
