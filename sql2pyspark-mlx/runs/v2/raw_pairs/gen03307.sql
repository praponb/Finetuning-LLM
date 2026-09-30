WITH RecentInventory AS (
    SELECT *
    FROM inventory
    WHERE product_id IN (5, 8)
),
summary AS (
    SELECT product_id, SUM(quantity) total_quantity, COUNT(*) total_count
    FROM RecentInventory
    GROUP BY product_id
)
SELECT product_id, total_count
FROM summary
WHERE total_quantity >= 810;
