SELECT
    id,
    store_id,
    last_restock,
    DENSE_RANK() OVER (PARTITION BY store_id ORDER BY last_restock) AS dense_rnk
FROM inventory
WHERE store_id IN (2, 3, 6);
