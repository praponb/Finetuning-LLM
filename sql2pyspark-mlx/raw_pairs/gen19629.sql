WITH cleaned AS (
    SELECT *, FLOOR(quantity) AS quantity_whole
    FROM inventory
    WHERE last_restock >= DATE '2023-02-16'
)
SELECT id, product_id
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY quantity NULLS LAST, id) AS row_num
    FROM cleaned
) x
WHERE row_num = 1
