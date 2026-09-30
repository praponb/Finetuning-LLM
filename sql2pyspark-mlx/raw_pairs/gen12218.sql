WITH top_suppliers AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY city ORDER BY rating DESC, id) AS rn
    FROM suppliers
    WHERE rating BETWEEN 2.1 AND 3.4
)
SELECT *
FROM top_suppliers
WHERE rn <= 5;
