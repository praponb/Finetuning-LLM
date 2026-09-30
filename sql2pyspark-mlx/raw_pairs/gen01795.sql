WITH ranked_suppliers AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY city ORDER BY rating DESC, id) AS pos
    FROM suppliers
)
SELECT id, city, rating
FROM ranked_suppliers
WHERE pos <= 2
