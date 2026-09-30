WITH base AS (
    SELECT *
    FROM order_items
    WHERE unit_price < 120
),
product_id_summary AS (
    SELECT product_id, order_id, ROUND(AVG(unit_price), 2) AS avg_unit_price
    FROM base
    GROUP BY product_id, order_id
)
SELECT *
FROM product_id_summary
ORDER BY order_id DESC
