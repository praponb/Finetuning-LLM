SELECT
    id,
    product_id,
    last_restock,
    store_id,
    FIRST_VALUE(store_id) OVER (PARTITION BY product_id ORDER BY last_restock, id) AS first_store_id
FROM inventory
ORDER BY last_restock, id DESC;
