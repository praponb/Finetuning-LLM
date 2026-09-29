SELECT
    inventory.id,
    inventory.last_restock,
    inventory.store_id,
    store_sales.sale_date,
    store_sales.store_id AS store_sale_store_id,
    store_sales.units,
    DATEDIFF(CURRENT_DATE(), inventory.last_restock) AS days_since_last_restock
FROM inventory
LEFT OUTER JOIN store_sales ON inventory.store_id = store_sales.store_id AND inventory.product_id = store_sales.product_id
WHERE store_sales.product_id IN (8, 9) OR store_sales.units <= 22
ORDER BY inventory.id DESC;
