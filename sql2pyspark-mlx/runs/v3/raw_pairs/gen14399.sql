WITH inventory_by_store AS (
    SELECT store_id, COUNT(*) AS num_inventory, COUNT(DISTINCT quantity) AS unique_products
    FROM inventory
    GROUP BY store_id
)
SELECT
    s.store_name,
    s.city,
    ibs.num_inventory,
    ibs.unique_products
FROM stores s
LEFT JOIN inventory_by_store ibs ON s.id = ibs.store_id
ORDER BY s.store_name DESC;
