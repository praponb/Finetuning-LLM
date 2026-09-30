SELECT
    id,
    product_id,
    last_restock,
    ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY last_restock DESC, id) AS rn,
    COUNT(*) OVER (PARTITION BY product_id) AS product_id_count
FROM inventory
ORDER BY id, last_restock DESC
