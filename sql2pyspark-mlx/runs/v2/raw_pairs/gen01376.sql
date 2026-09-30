WITH InventoryByStore AS (
    SELECT store_id, MAX(quantity) - MIN(quantity) quantity_range
    FROM inventory
    GROUP BY store_id
)
SELECT s.sqft, s.store_name, I.quantity_range
FROM stores s
JOIN InventoryByStore I ON s.id = I.store_id
ORDER BY s.sqft DESC, s.store_name DESC;
