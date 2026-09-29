SELECT
    id,
    product_id,
    last_restock,
    quantity
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY last_restock DESC, id) AS row_num
    FROM inventory
) AS ranked
WHERE row_num <= 5
ORDER BY quantity
