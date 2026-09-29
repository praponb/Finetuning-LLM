SELECT
    id,
    product_id,
    last_restock,
    store_id
FROM (
    SELECT *, DENSE_RANK() OVER (PARTITION BY product_id ORDER BY last_restock DESC) AS row_num
    FROM inventory
) AS r
WHERE row_num <= 1
ORDER BY last_restock DESC, id DESC;
