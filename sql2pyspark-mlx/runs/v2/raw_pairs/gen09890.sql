SELECT
    id,
    country,
    rating,
    supplier_name
FROM (
    SELECT *, DENSE_RANK() OVER (PARTITION BY country ORDER BY rating DESC) AS rn
    FROM suppliers
) AS r
WHERE rn <= 2;
