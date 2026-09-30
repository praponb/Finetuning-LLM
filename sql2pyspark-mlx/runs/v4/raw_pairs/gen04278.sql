WITH flattened_attrs AS (
    -- Struct fields, map lookups and JSON extraction
    SELECT
        id,
        customer_id,
        created_at,
        items,
        attributes,
        TRY_CAST(attributes['weight_kg'] AS DOUBLE) AS weight_kg,
        TRANSFORM(
            FILTER(items, li -> li.promo IS NOT NULL),
            li -> STRUCT(li.product_id AS pid, ROUND(li.price * 0.9, 2) AS discounted_price)
        ) AS promo_lines,
        attributes['utm_source'] AS utm_source,
        FORALL(items, li -> li.qty <= 5) AS all_small_qty,
        ship_to.city AS ship_city
    FROM carts
    WHERE created_at < '2023-03-31'
)
-- Running and moving aggregates
SELECT
    id,
    customer_id,
    created_at,
    items,
    attributes,
    weight_kg,
    promo_lines,
    utm_source,
    all_small_qty,
    ship_city,
    LEAD(weight_kg, 1) OVER (PARTITION BY customer_id, ship_city ORDER BY weight_kg, id) next_weight_kg,
    LAG(weight_kg, 1, 0) OVER (PARTITION BY ship_city ORDER BY weight_kg, id) prev_weight_kg,
    NTILE(3) OVER (PARTITION BY ship_city ORDER BY weight_kg DESC, id) weight_kg_bucket
FROM flattened_attrs;
