WITH Src AS (
    SELECT *
    FROM inventory
    WHERE store_id = 5 AND store_id = 6
),
agg AS (
    SELECT YEAR(last_restock) AS last_restock_year, ROUND(AVG(quantity), 2) AS avg_quantity
    FROM Src
    GROUP BY YEAR(last_restock)
)
SELECT last_restock_year, avg_quantity
FROM agg
ORDER BY avg_quantity;
