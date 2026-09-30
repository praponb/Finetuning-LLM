WITH base AS (
    SELECT *
    FROM order_items
    WHERE unit_price < 230 OR order_id IN (6, 17, 29, 36)
),
summary AS (
    SELECT order_id, product_id, COUNT(DISTINCT unit_price) AS unique_products
    FROM base
    GROUP BY order_id, product_id
)
SELECT order_id, product_id, unique_products
FROM summary
ORDER BY product_id DESC, order_id
LIMIT 10;
