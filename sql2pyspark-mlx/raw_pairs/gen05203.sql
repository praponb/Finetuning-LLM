SELECT
    id,
    country,
    city,
    rating,
    supplier_name,
    NTILE(10) OVER (PARTITION BY country, city ORDER BY rating, id) AS rating_ntile,
    FIRST_VALUE(supplier_name) OVER (PARTITION BY city ORDER BY rating, id) AS first_supplier_name
FROM suppliers
WHERE rating > 3.5
ORDER BY id DESC
