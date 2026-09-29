WITH valid_inventory AS (
    SELECT *
    FROM inventory
    WHERE store_id IN (1, 3)
),
summary AS (
    SELECT product_id, YEAR(last_restock) AS last_restock_year, SUM(quantity) AS total_quantity
    FROM valid_inventory
    GROUP BY product_id, YEAR(last_restock)
)
SELECT *
FROM summary
WHERE total_quantity >= 460
ORDER BY product_id DESC, total_quantity DESC;
