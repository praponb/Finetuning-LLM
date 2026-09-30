WITH StoreStats AS (
    SELECT store_id, AVG(quantity) AS avg_quantity
    FROM inventory
    GROUP BY store_id
)
SELECT
    s.id,
    s.store_name,
    s.city,
    S.avg_quantity
FROM stores s
INNER JOIN StoreStats S ON s.id = S.store_id
ORDER BY S.avg_quantity;
