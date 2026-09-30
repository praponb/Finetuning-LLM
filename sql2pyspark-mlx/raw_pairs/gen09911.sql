SELECT
    id,
    AGGREGATE(items, 0.0D, (acc, item) -> acc + item.qty * item.price, acc -> ROUND(acc, 2)) AS cart_value,
    TRY_CAST(attributes['weight_kg'] AS DOUBLE) AS weight_kg,
    COALESCE(attributes['utm_source'], 'direct') AS utm_source_or_default
FROM carts
