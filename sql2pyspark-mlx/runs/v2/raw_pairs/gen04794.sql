WITH valid_inventory AS (
    SELECT *
    FROM inventory
    WHERE NOT (store_id IN (2, 5, 6) AND store_id IN (3, 6))
),
summary AS (
    SELECT product_id, MAX(quantity) - MIN(quantity) quantity_range, AVG(quantity) avg_quantity
    FROM valid_inventory
    GROUP BY product_id
)
SELECT *
FROM summary
ORDER BY avg_quantity, product_id
LIMIT 10
