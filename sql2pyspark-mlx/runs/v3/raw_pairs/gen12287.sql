WITH src AS (
    SELECT *
    FROM order_items
    WHERE quantity IS NOT NULL AND product_id IN (1, 18, 24, 40)
),
order_items_stats AS (
    SELECT
        order_id,
        product_id,
        MAX(quantity) - MIN(quantity) AS quantity_range,
        AVG(unit_price) AS avg_unit_price
    FROM src
    GROUP BY order_id, product_id
)
SELECT
    order_id,
    product_id,
    quantity_range,
    avg_unit_price
FROM order_items_stats
ORDER BY order_id DESC
