SELECT
    stores.region,
    stores.city,
    SUM(inventory.quantity) AS total_quantity,
    MIN(inventory.quantity) AS min_quantity
FROM inventory
LEFT JOIN stores ON inventory.store_id = stores.id
WHERE stores.region IS NULL
GROUP BY stores.region, stores.city
ORDER BY stores.region
