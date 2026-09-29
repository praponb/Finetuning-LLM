SELECT
    inventory.id,
    inventory.last_restock,
    inventory.store_id,
    inventory.product_id,
    store_sales.store_id AS store_sale_store_id,
    store_sales.product_id AS store_sale_product_id,
    store_sales.sale_date
FROM inventory
INNER JOIN store_sales ON inventory.store_id = store_sales.store_id AND inventory.product_id = store_sales.product_id
WHERE store_sales.store_id IN (2, 4) AND inventory.quantity BETWEEN 81 AND 225;
