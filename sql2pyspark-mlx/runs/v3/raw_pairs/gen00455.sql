SELECT
    inventory.product_id,
    inventory.store_id,
    stores.sqft,
    stores.store_name,
    products.price,
    products.cost,
    ADD_MONTHS(inventory.last_restock, 3) AS last_restock_plus_months
FROM inventory
RIGHT JOIN stores ON stores.id = inventory.store_id
RIGHT JOIN products ON inventory.product_id = products.id
