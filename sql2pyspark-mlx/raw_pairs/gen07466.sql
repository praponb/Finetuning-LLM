SELECT
    id,
    product_id,
    quantity,
    store_id
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY quantity, id) AS row_num
    FROM inventory
) AS ranked
WHERE row_num <= 3
ORDER BY store_id
