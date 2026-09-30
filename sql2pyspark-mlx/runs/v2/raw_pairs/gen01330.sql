WITH src AS (
    SELECT *
    FROM inventory
    WHERE store_id IN (1, 3, 6) AND product_id = 5 AND product_id = 3
),
Agg AS (
    SELECT MONTH(last_restock) AS last_restock_month, AVG(quantity) AS avg_quantity, MAX(quantity) - MIN(quantity) AS quantity_range
    FROM src
    GROUP BY MONTH(last_restock)
)
SELECT *
FROM Agg
WHERE quantity_range >= 43
ORDER BY avg_quantity DESC, last_restock_month
LIMIT 5
