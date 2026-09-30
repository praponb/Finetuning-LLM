WITH src AS (
    SELECT *, CEIL(quantity) AS quantity_whole
    FROM inventory
    WHERE YEAR(last_restock) = 2023
),
inventory_stats AS (
    SELECT CASE WHEN quantity >= 259 THEN 'high' WHEN quantity >= 74 THEN 'mid' ELSE 'low' END AS quantity_bucket, MAX(quantity_whole) AS max_quantity_whole, ROUND(AVG(quantity), 2) AS avg_quantity
    FROM src
    GROUP BY CASE WHEN quantity >= 259 THEN 'high' WHEN quantity >= 74 THEN 'mid' ELSE 'low' END
)
SELECT quantity_bucket, max_quantity_whole, avg_quantity
FROM inventory_stats
WHERE avg_quantity >= 23;
