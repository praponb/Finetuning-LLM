SELECT
    inventory.id,
    inventory.quantity,
    inventory.product_id,
    inventory.store_id
FROM inventory
WHERE inventory.product_id IN (1, 4, 7, 9) AND inventory.product_id NOT IN (SELECT id FROM products WHERE brand != 'Stark')
