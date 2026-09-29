SELECT
    inventory.id,
    inventory.last_restock,
    products.stock_qty,
    products.product_name,
    products.brand
FROM inventory
INNER JOIN products ON inventory.product_id = products.id
WHERE inventory.product_id = 9
ORDER BY product_name;
