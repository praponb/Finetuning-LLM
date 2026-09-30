WITH valid_inventory AS (
    SELECT *
    FROM inventory
    WHERE YEAR(last_restock) = 2025
),
agg AS (
    SELECT store_id, AVG(quantity) AS avg_quantity, MAX(quantity) - MIN(quantity) AS quantity_range
    FROM valid_inventory
    GROUP BY store_id
)
SELECT store_id, avg_quantity, quantity_range
FROM agg
ORDER BY avg_quantity ASC, store_id DESC;
