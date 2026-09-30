WITH Numbered AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY city ORDER BY rating, id) AS pos
    FROM suppliers
)
SELECT
    id,
    city,
    rating,
    supplier_name
FROM Numbered
WHERE pos <= 5
