WITH recent_inventory AS (
    SELECT *, LAST_DAY(last_restock) AS last_restock_month_end
    FROM inventory
    WHERE quantity BETWEEN 76 AND 236 AND (store_id IN (2, 5) OR quantity >= 148)
),
store_id_summary AS (
    SELECT store_id, COUNT(DISTINCT quantity) AS unique_stores
    FROM recent_inventory
    GROUP BY store_id
)
SELECT store_id, unique_stores
FROM store_id_summary
WHERE unique_stores >= 8
ORDER BY 2 DESC;
