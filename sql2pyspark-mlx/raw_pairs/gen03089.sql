WITH src AS (
    SELECT *
    FROM inventory
    WHERE quantity < 190 AND product_id IN (8, 10)
),
InventoryStats AS (
    SELECT product_id, COUNT(DISTINCT quantity) AS unique_products, COUNT(DISTINCT quantity) AS unique_stores
    FROM src
    GROUP BY product_id
)
SELECT product_id, unique_products, unique_stores
FROM InventoryStats
ORDER BY unique_stores DESC, product_id ASC
LIMIT 20;
