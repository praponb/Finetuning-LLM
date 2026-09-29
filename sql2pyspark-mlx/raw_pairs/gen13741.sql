SELECT
    id,
    product_id,
    quantity,
    store_id
FROM inventory
WHERE store_id = 1
ORDER BY store_id DESC;
