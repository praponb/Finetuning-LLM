SELECT
    id,
    supplier_name,
    country,
    LOWER(city) city_lower,
    COALESCE(rating, 0) rating_filled
FROM suppliers
WHERE city IN ('New York', 'London')
ORDER BY city_lower, supplier_name DESC, id
LIMIT 5;
