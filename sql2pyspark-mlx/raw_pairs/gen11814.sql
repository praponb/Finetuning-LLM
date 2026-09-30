SELECT
    id,
    customer_id,
    created_at,
    ship_to,
    attributes,
    attributes['utm_source'] AS utm_source,
    TRANSFORM(items, li -> li.product_id) AS product_ids
FROM carts
WHERE created_at >= '2025-09-29';
