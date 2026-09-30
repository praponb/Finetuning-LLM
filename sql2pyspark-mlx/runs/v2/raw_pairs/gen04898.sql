WITH Base AS (
    SELECT *, CAST(quantity AS STRING) AS quantity_str
    FROM inventory
    WHERE store_id IN (1, 3, 6) AND last_restock < '2024-01-16' AND store_id = 1
),
StoreIdSummary AS (
    SELECT store_id, product_id, ROUND(AVG(quantity), 2) AS avg_quantity
    FROM Base
    GROUP BY store_id, product_id
)
SELECT *
FROM StoreIdSummary
ORDER BY product_id;
