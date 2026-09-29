WITH inventory_by_store AS (
    SELECT store_id, MIN(last_restock) AS min_last_restock, AVG(quantity) AS avg_quantity
    FROM inventory
    GROUP BY store_id
)
SELECT
    s.id,
    s.store_name,
    ibs.min_last_restock,
    ibs.avg_quantity
FROM stores s
LEFT JOIN inventory_by_store ibs ON s.id = ibs.store_id
WHERE ibs.avg_quantity > 210
ORDER BY id
