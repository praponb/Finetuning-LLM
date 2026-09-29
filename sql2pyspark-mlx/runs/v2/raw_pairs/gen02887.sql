WITH Numbered AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY quantity DESC, id) AS row_num
    FROM inventory
)
SELECT id, product_id, quantity
FROM Numbered
WHERE row_num <= 5
ORDER BY id, quantity DESC;
