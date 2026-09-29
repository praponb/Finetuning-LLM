SELECT
    id,
    country,
    rating,
    supplier_name
FROM (
    SELECT *, DENSE_RANK() OVER (PARTITION BY country ORDER BY rating DESC) AS rnk
    FROM suppliers
) AS r
WHERE rnk <= 3
ORDER BY country DESC
