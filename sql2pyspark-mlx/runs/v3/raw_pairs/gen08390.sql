SELECT *
FROM (
    SELECT
        id,
        product_id,
        last_restock,
        quantity,
        ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY last_restock, id) row_num
    FROM inventory
) ranked
WHERE row_num <= 1
ORDER BY product_id, row_num DESC;
