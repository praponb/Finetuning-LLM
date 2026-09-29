WITH valid_inventory AS (
    SELECT *
    FROM inventory
    WHERE (last_restock BETWEEN '2023-09-13' AND '2025-01-15' AND product_id IN (1, 3, 9)) OR product_id = 7
),
summary AS (
    SELECT store_id, AVG(quantity) AS avg_quantity, MIN(quantity) AS min_quantity
    FROM valid_inventory
    GROUP BY store_id
)
SELECT store_id, min_quantity
FROM summary;
