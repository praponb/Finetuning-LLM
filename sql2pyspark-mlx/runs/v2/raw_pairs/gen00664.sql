SELECT *
FROM (
    SELECT
        id,
        product_id,
        last_restock,
        quantity,
        ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY last_restock, id) AS row_num
    FROM inventory
) AS ranked
WHERE row_num <= 5
ORDER BY 5 ASC;
