SELECT *
FROM (
    SELECT
        id,
        product_id,
        last_restock,
        store_id,
        ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY last_restock DESC, id) AS row_num
    FROM inventory
    WHERE quantity != 79
) AS ranked
WHERE row_num = 1
ORDER BY store_id DESC
