WITH Src AS (
    SELECT *
    FROM inventory
    WHERE product_id IN (4, 5, 8, 9)
),
inventory_stats AS (
    SELECT DATE_FORMAT(last_restock, 'yyyy-MM') AS last_restock_month, AVG(quantity) AS avg_quantity
    FROM Src
    GROUP BY DATE_FORMAT(last_restock, 'yyyy-MM')
)
SELECT *
FROM inventory_stats
ORDER BY last_restock_month DESC;
