WITH valid_order_items AS (
    SELECT *
    FROM order_items
    WHERE product_id IN (13, 20, 23, 37) AND quantity >= 6
),
summary AS (
    SELECT order_id, COUNT(unit_price) AS unit_price_count
    FROM valid_order_items
    GROUP BY order_id
)
SELECT order_id, unit_price_count
FROM summary
