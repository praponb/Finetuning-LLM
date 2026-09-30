SELECT
    inventory.store_id,
    stores.store_name,
    products.price,
    products.stock_qty
FROM inventory
LEFT JOIN stores ON inventory.store_id = stores.id
LEFT JOIN products ON inventory.product_id = products.id
WHERE inventory.store_id IN (1, 2, 3, 5)
ORDER BY inventory.store_id DESC, stock_qty
