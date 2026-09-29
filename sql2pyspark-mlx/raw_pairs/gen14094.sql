SELECT
    inventory.id,
    inventory.product_id,
    products.category,
    stores.store_name
FROM inventory
LEFT OUTER JOIN products ON products.id = inventory.product_id
LEFT OUTER JOIN stores ON inventory.store_id = stores.id
