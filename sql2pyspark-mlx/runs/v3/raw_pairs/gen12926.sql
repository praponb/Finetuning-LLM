WITH inventory_by_store AS (
    SELECT store_id, AVG(quantity) AS avg_quantity, MAX(last_restock) AS latest_last_restock
    FROM inventory
    GROUP BY store_id
)
SELECT
    s.id,
    s.region,
    s.opened_date,
    ibs.avg_quantity,
    ibs.latest_last_restock
FROM stores s
INNER JOIN inventory_by_store ibs ON s.id = ibs.store_id
WHERE ibs.avg_quantity >= 70
ORDER BY avg_quantity DESC, region, id
LIMIT 50
