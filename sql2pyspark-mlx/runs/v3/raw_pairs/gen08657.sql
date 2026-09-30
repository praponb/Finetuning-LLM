SELECT
    inventory.id,
    inventory.quantity,
    inventory.store_id,
    inventory.last_restock,
    store_sales.product_id,
    store_sales.store_id AS store_sale_store_id
FROM inventory
LEFT JOIN store_sales ON inventory.store_id = store_sales.store_id AND inventory.product_id = store_sales.product_id
WHERE store_sales.store_id IN (2, 3, 4, 5) AND inventory.quantity >= 208
ORDER BY product_id DESC, store_sale_store_id;
