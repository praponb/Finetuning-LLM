WITH StoreTotals AS (
    SELECT store_id, COUNT(*) AS num_inventory, SUM(quantity) AS total_quantity
    FROM inventory
    GROUP BY store_id
)
SELECT
    s.id,
    s.opened_date,
    s.sqft,
    S.num_inventory,
    S.total_quantity
FROM stores s
LEFT JOIN StoreTotals S ON s.id = S.store_id
ORDER BY id
