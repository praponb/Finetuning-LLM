WITH TopProducts AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY brand ORDER BY cost DESC, id) AS pos
    FROM products
    WHERE cost > 380
)
SELECT *
FROM TopProducts
WHERE pos <= 2
ORDER BY pos DESC
