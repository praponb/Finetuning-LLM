SELECT
    inventory.last_restock,
    stores.city,
    stores.opened_date,
    CASE WHEN inventory.quantity >= 209 THEN 'High' WHEN inventory.quantity >= 65 THEN 'Medium' ELSE 'Low' END AS quantity_level
FROM inventory
INNER JOIN stores ON inventory.store_id = stores.id
WHERE stores.city = 'London'
