WITH src AS (
    SELECT *
    FROM order_items
    WHERE product_id IS NULL
),
order_items_stats AS (
    SELECT order_id, AVG(unit_price) AS avg_unit_price, COUNT(DISTINCT unit_price) AS unique_orders
    FROM src
    GROUP BY order_id
)
SELECT *
FROM order_items_stats
