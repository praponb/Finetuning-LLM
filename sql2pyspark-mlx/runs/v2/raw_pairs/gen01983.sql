WITH top_suppliers AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY city ORDER BY rating DESC, id) AS pos
    FROM suppliers
    WHERE rating >= 4.0
)
SELECT *
FROM top_suppliers
WHERE pos <= 3
ORDER BY rating, 3;
