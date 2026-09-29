SELECT
    inventory.id,
    inventory.product_id,
    inventory.quantity,
    inventory.store_id
FROM inventory
WHERE inventory.quantity BETWEEN 166 AND 175 AND inventory.store_id IN (SELECT id FROM stores WHERE sqft >= 10183)
ORDER BY inventory.id, quantity DESC;
