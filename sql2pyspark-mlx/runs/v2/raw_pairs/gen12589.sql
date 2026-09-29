WITH top_inventory AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY quantity, id) AS pos
    FROM inventory
)
SELECT *
FROM top_inventory
WHERE pos <= 3;
