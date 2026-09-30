WITH base AS (
    SELECT *
    FROM order_items
    WHERE product_id IN (5, 17, 36) AND product_id = 13 AND product_id IS NOT NULL
),
summary AS (
    SELECT order_id, MAX(quantity) - MIN(quantity) quantity_range
    FROM base
    GROUP BY order_id
)
SELECT order_id, quantity_range
FROM summary
WHERE quantity_range > 43
ORDER BY 2 DESC, order_id
LIMIT 1;
