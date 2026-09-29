SELECT
    id,
    country,
    rating,
    LEAD(rating, 1) OVER (PARTITION BY country ORDER BY rating, id) AS next_rating
FROM suppliers
WHERE LOWER(supplier_name) LIKE '%oky%'
ORDER BY country;
