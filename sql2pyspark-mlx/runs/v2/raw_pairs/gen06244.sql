WITH inventory_by_store AS (
    SELECT store_id, MAX(quantity) AS max_quantity, COUNT(DISTINCT quantity) AS unique_stores
    FROM inventory
    WHERE last_restock < '2024-03-19'
    GROUP BY store_id
)
SELECT
    s.id,
    s.store_name,
    s.opened_date,
    ibs.max_quantity,
    ibs.unique_stores
FROM stores s
LEFT JOIN inventory_by_store ibs ON s.id = ibs.store_id
