WITH valid_inventory AS (
    SELECT *
    FROM inventory
    WHERE (product_id IN (2, 5, 6, 7) AND product_id IN (1, 3, 7)) OR YEAR(last_restock) >= 2023
),
summary AS (
    SELECT CASE WHEN quantity >= 264 THEN 'high' WHEN quantity >= 30 THEN 'mid' ELSE 'low' END AS quantity_bucket, SUM(quantity) AS total_quantity
    FROM valid_inventory
    GROUP BY CASE WHEN quantity >= 264 THEN 'high' WHEN quantity >= 30 THEN 'mid' ELSE 'low' END
)
SELECT *
FROM summary
WHERE total_quantity >= 470
ORDER BY quantity_bucket DESC, total_quantity;
