SELECT
    id,
    city,
    rating,
    LAG(rating, 1, 0) OVER (PARTITION BY city ORDER BY rating, id) prev_rating
FROM suppliers
WHERE LOWER(TRIM(supplier_name)) = 'global supply';
