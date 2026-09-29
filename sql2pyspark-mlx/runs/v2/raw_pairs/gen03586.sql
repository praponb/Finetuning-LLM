WITH filtered_order_items AS (
    SELECT *, COALESCE(quantity, 0) AS quantity_filled
    FROM order_items
    WHERE (quantity <> 6 AND order_id IN (12, 23, 26)) OR unit_price < 290
),
agg AS (
    SELECT
        CASE WHEN unit_price >= 330 THEN 'high' WHEN unit_price >= 130 THEN 'mid' ELSE 'low' END AS unit_price_bucket,
        order_id,
        MAX(quantity) - MIN(quantity) AS quantity_range,
        MAX(unit_price) AS max_unit_price
    FROM filtered_order_items
    GROUP BY CASE WHEN unit_price >= 330 THEN 'high' WHEN unit_price >= 130 THEN 'mid' ELSE 'low' END, order_id
)
SELECT
    unit_price_bucket,
    order_id,
    quantity_range,
    max_unit_price
FROM agg
WHERE max_unit_price > 320;
