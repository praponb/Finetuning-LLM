WITH valid_inventory AS (
    SELECT *
    FROM inventory
    WHERE store_id IN (2, 3, 4, 6) OR last_restock <= '2025-02-01'
),
summary AS (
    SELECT product_id, MAX(last_restock) AS last_last_restock
    FROM valid_inventory
    GROUP BY product_id
)
SELECT *
FROM summary;
