SELECT
    id,
    product_id,
    quantity,
    store_id,
    LAST_VALUE(store_id) OVER (PARTITION BY product_id ORDER BY quantity DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_store_id,
    FIRST_VALUE(store_id) OVER (PARTITION BY product_id ORDER BY quantity, id) AS first_store_id
FROM inventory
WHERE product_id IN (3, 4, 7)
