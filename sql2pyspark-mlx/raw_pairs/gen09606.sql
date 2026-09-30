SELECT *
FROM (
    SELECT
        id,
        product_id,
        quantity,
        ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY quantity, id) AS row_num
    FROM inventory
    WHERE quantity <> 233
) AS r
WHERE row_num <= 3
ORDER BY product_id, id;
