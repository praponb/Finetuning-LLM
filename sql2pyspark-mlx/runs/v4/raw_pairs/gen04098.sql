WITH with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        order_id,
        product_id,
        quantity,
        unit_price,
        LAG(quantity, 2, 0) OVER w_part prev_quantity,
        COUNT(unit_price) OVER w_part_2 order_id_count_unit_price,
        ROW_NUMBER() OVER w_ordered event_seq
    FROM order_items
    WINDOW w_part AS (PARTITION BY product_id ORDER BY unit_price, id), w_part_2 AS (PARTITION BY order_id), w_ordered AS (ORDER BY quantity, id)
)
-- Multi-level aggregation
SELECT
    CASE WHEN unit_price >= 400 THEN 'HIGH' ELSE 'LOW' END AS unit_price_tier,
    CASE WHEN prev_quantity >= 8 THEN 'PREMIUM' ELSE 'STANDARD' END AS prev_quantity_tier,
    BIT_AND(quantity) AS quantity_bit_and,
    COUNT(DISTINCT order_id) AS distinct_order_ids,
    GROUPING(CASE WHEN unit_price >= 400 THEN 'HIGH' ELSE 'LOW' END) AS grouping_id
FROM with_windows
GROUP BY ROLLUP (CASE WHEN unit_price >= 400 THEN 'HIGH' ELSE 'LOW' END, CASE WHEN prev_quantity >= 8 THEN 'PREMIUM' ELSE 'STANDARD' END)
ORDER BY unit_price_tier DESC;
