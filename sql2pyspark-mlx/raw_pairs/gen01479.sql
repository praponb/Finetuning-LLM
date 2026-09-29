SELECT
    id,
    country,
    rating,
    supplier_name
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY country ORDER BY rating DESC, id) AS pos
    FROM suppliers
) AS ranked
WHERE pos = 1
