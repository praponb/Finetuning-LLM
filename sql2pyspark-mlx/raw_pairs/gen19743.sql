SELECT
    id,
    product_id,
    quantity,
    last_restock
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY quantity DESC, id) AS row_num
    FROM inventory
    WHERE store_id IN (2, 3, 4, 6)
) AS r
WHERE row_num <= 5
