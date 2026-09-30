WITH inventory_by_store AS (
    SELECT store_id, SUM(quantity) total_quantity
    FROM inventory
    GROUP BY store_id
)
SELECT
    s.id,
    s.sqft,
    s.opened_date,
    ibs.total_quantity
FROM stores s
JOIN inventory_by_store ibs ON s.id = ibs.store_id
ORDER BY opened_date;
