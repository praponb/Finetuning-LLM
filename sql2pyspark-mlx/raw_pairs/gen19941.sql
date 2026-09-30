SELECT
    id,
    customer_id,
    created_at,
    ship_to,
    attributes,
    SIZE(attributes) AS num_attributes,
    ARRAY_CONTAINS(MAP_KEYS(attributes), 'weight_kg') AS has_weight_kg,
    ship_to.zip AS ship_zip
FROM carts
WHERE created_at >= '2023-05-01';
