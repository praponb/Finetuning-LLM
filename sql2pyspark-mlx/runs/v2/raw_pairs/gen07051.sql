WITH valid_order_items AS (
    SELECT *, ROUND(unit_price * 1.15, 2) AS unit_price_adjusted
    FROM order_items
    WHERE quantity = 4 AND order_id IN (7, 18, 19, 40)
),
product_id_summary AS (
    SELECT product_id, COUNT(DISTINCT quantity) AS unique_orders
    FROM valid_order_items
    GROUP BY product_id
)
SELECT *
FROM product_id_summary
WHERE unique_orders >= 9
ORDER BY unique_orders DESC
