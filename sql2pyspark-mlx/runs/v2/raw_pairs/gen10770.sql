WITH top_suppliers AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY city ORDER BY rating DESC, id) AS rnk
    FROM suppliers
)
SELECT *
FROM top_suppliers
WHERE rnk <= 3
ORDER BY rating DESC
