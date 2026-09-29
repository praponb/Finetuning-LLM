SELECT
    id,
    country,
    rating,
    supplier_name
FROM (
    SELECT *, DENSE_RANK() OVER (PARTITION BY country ORDER BY rating) row_num
    FROM suppliers
) t
WHERE row_num <= 3;
