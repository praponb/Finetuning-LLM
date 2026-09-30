WITH filtered_inventory AS (
    SELECT *
    FROM inventory
    WHERE last_restock > '2023-06-13'
),
summary AS (
    SELECT store_id, COUNT(DISTINCT quantity) AS unique_products, AVG(quantity) AS avg_quantity
    FROM filtered_inventory
    GROUP BY store_id
)
SELECT store_id, unique_products, avg_quantity
FROM summary
WHERE unique_products < 8
ORDER BY avg_quantity;
