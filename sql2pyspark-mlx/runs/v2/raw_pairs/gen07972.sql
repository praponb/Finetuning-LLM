SELECT
    id,
    country,
    rating,
    supplier_name
FROM (
    SELECT
        id,
        country,
        rating,
        supplier_name,
        DENSE_RANK() OVER (PARTITION BY country ORDER BY rating DESC) row_num
    FROM suppliers
) t
WHERE row_num <= 2
ORDER BY supplier_name;
