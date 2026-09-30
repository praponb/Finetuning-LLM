WITH parsed AS (
    -- Higher-order array functions
    SELECT
        id,
        customer_id,
        items,
        ship_to,
        attributes['utm_source'] AS utm_source,
        EXISTS(items, x -> x.price > 100) AS has_premium_item,
        ship_to.zip AS ship_zip,
        TRY_CAST(attributes['weight_kg'] AS DOUBLE) AS weight_kg
    FROM carts
)
-- Running and moving aggregates
SELECT
    id,
    customer_id,
    utm_source,
    has_premium_item,
    weight_kg,
    LAG(weight_kg) OVER w_ordered AS prev_weight_kg,
    LEAD(weight_kg) OVER w_ordered AS next_weight_kg,
    ROW_NUMBER() OVER w_ordered AS seq_no
FROM parsed
WHERE has_premium_item
WINDOW w_ordered AS (PARTITION BY customer_id ORDER BY weight_kg, id)
