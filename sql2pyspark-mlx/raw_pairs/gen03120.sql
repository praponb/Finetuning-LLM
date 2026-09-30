SELECT
    id,
    store_id,
    quantity,
    last_restock,
    LAST_VALUE(last_restock) OVER (PARTITION BY store_id ORDER BY quantity DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_last_restock
FROM inventory
WHERE product_id = 6;
