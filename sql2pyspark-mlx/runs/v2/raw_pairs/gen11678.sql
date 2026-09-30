SELECT
    id,
    store_id,
    quantity,
    product_id
FROM (
    SELECT
        id,
        store_id,
        quantity,
        product_id,
        DENSE_RANK() OVER (PARTITION BY store_id ORDER BY quantity DESC) AS rnk
    FROM inventory
    WHERE store_id IN (4, 5)
) AS t
WHERE rnk = 1
ORDER BY 4 DESC;
