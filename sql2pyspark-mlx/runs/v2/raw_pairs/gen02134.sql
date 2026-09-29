WITH base AS (
    SELECT *
    FROM inventory
    WHERE quantity > 150 AND last_restock >= '2025-09-02'
),
summary AS (
    SELECT product_id, SUM(quantity) AS total_quantity
    FROM base
    GROUP BY product_id
)
SELECT product_id, total_quantity
FROM summary
WHERE total_quantity < 720
ORDER BY product_id DESC;
