SELECT
    stores.id,
    stores.sqft,
    stores.city,
    stores.region
FROM stores
WHERE EXISTS (SELECT 1 FROM inventory WHERE inventory.store_id = stores.id AND MONTH(inventory.last_restock) IN (7, 8, 10))
ORDER BY stores.id ASC, stores.region ASC;
