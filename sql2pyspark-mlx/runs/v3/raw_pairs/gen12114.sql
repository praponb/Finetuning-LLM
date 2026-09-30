WITH base AS (
    SELECT *
    FROM order_items
    WHERE quantity != 5 AND (quantity < 6 OR unit_price BETWEEN 240 AND 390)
),
summary AS (
    SELECT product_id, SUM(unit_price * quantity) AS revenue, MIN(quantity) AS min_quantity
    FROM base
    GROUP BY product_id
)
SELECT *
FROM summary
ORDER BY min_quantity DESC
