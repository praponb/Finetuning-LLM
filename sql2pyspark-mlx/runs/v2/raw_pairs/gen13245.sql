WITH inventory_by_store AS (
    SELECT store_id, COUNT(quantity) AS quantity_count
    FROM inventory
    WHERE last_restock < '2023-10-13'
    GROUP BY store_id
)
SELECT stores.id, stores.opened_date, inventory_by_store.quantity_count
FROM stores
JOIN inventory_by_store ON stores.id = inventory_by_store.store_id
ORDER BY inventory_by_store.quantity_count, id
