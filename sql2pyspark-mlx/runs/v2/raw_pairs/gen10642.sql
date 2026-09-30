WITH recent_order_items AS (
    SELECT *
    FROM order_items
    WHERE product_id = 32 AND (product_id IN (4, 9, 14, 18) OR quantity IS NOT NULL)
),
summary AS (
    SELECT product_id, SUM(unit_price) total_unit_price
    FROM recent_order_items
    GROUP BY product_id
)
SELECT product_id, total_unit_price
FROM summary
ORDER BY total_unit_price, product_id
LIMIT 5
