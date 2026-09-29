WITH recent_order_items AS (
    SELECT *
    FROM order_items
    WHERE quantity IS NOT NULL AND order_id IN (6, 14)
),
agg AS (
    SELECT order_id, SUM(unit_price) AS total_unit_price, MAX(unit_price) AS max_unit_price
    FROM recent_order_items
    GROUP BY order_id
)
SELECT order_id, total_unit_price, max_unit_price
FROM agg
WHERE max_unit_price > 160
