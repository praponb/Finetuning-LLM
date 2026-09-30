SELECT
    inventory.id,
    inventory.product_id,
    stores.opened_date,
    stores.sqft
FROM inventory
RIGHT JOIN stores ON inventory.store_id = stores.id
WHERE stores.sqft > 4436
ORDER BY opened_date, sqft DESC;
