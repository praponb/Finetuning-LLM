WITH unpacked AS (
    -- Struct fields, map lookups and JSON extraction
    SELECT
        id,
        items,
        ship_to,
        attributes,
        attributes['gift'] AS gift,
        SIZE(items) AS num_items,
        ARRAY_MAX(TRANSFORM(items, item -> item.price)) AS max_item_price,
        TRY_CAST(attributes['weight_kg'] AS DOUBLE) AS weight_kg
    FROM carts
)
-- Running and moving aggregates
SELECT
    id,
    gift,
    num_items,
    weight_kg,
    MAX(num_items) OVER w_ordered AS last7_max_num_items,
    COUNT(num_items) OVER w_ordered AS last7_count_num_items,
    AVG(num_items) OVER w_ordered AS last7_avg_num_items,
    NTILE(3) OVER (ORDER BY num_items DESC, id) AS num_items_bucket
FROM unpacked
WHERE max_item_price IS NOT NULL
WINDOW w_ordered AS (ORDER BY num_items, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW);
