WITH unpacked AS (
    SELECT
        id,
        customer_id,
        created_at,
        ship_to,
        EXISTS(items, x -> x.price > 100) AS has_premium_item,
        COALESCE(attributes['gift'], 'direct') AS gift_or_default,
        COALESCE(attributes['utm_source'], 'unknown') AS utm_source_or_default,
        TRY_CAST(attributes['weight_kg'] AS DOUBLE) AS weight_kg
    FROM carts
    WHERE customer_id IS NOT NULL
)
SELECT
    *,
    LEAD(weight_kg) OVER (PARTITION BY customer_id ORDER BY created_at ASC, id ASC) AS next_weight_kg,
    LAG(weight_kg) OVER (PARTITION BY customer_id ORDER BY created_at ASC, id ASC) AS prev_weight_kg,
    ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY created_at ASC, id ASC) AS seq_no
FROM unpacked;
