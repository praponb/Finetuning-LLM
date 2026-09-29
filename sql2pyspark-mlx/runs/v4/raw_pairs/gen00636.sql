SELECT
    id,
    customer_id,
    created_at,
    coupon_codes,
    attributes,
    SIZE(items) AS num_items,
    COALESCE(attributes['utm_source'], 'none') AS utm_source_or_default
FROM carts
WHERE created_at < '2023-11-01';
