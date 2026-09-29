WITH top_suppliers AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY city ORDER BY rating DESC, id) AS rn
    FROM suppliers
)
SELECT *
FROM top_suppliers
WHERE rn <= 3
ORDER BY supplier_name;
