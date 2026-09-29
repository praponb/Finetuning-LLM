WITH recent_order_items AS (
    SELECT *
    FROM order_items
    WHERE quantity >= 7 AND order_id IN (27, 37)
),
summary AS (
    SELECT order_id, AVG(unit_price) AS avg_unit_price, MIN(quantity) AS min_quantity
    FROM recent_order_items
    GROUP BY order_id
)
SELECT order_id, avg_unit_price, min_quantity
FROM summary
WHERE min_quantity < 7.0
ORDER BY min_quantity;
