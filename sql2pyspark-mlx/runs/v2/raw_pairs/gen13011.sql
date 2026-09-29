WITH TopSuppliers AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY city ORDER BY rating DESC, id) AS rnk
    FROM suppliers
)
SELECT id, city, rating
FROM TopSuppliers
WHERE rnk = 1
ORDER BY id DESC
