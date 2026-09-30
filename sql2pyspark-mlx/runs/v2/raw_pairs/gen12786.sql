SELECT
    stores.id,
    stores.store_name,
    stores.region,
    stores.city
FROM stores
WHERE stores.opened_date < '2025-01-01' AND NOT EXISTS (SELECT 1 FROM inventory WHERE inventory.store_id = stores.id AND MONTH(inventory.last_restock) IN (2, 7, 11));
