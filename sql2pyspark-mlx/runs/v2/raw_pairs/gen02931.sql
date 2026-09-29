SELECT
    id,
    product_id,
    quantity,
    last_restock
FROM (
    SELECT
        id,
        product_id,
        quantity,
        last_restock,
        RANK() OVER (PARTITION BY product_id ORDER BY quantity) AS row_num
    FROM inventory
) AS t
WHERE row_num <= 2
ORDER BY id ASC;
