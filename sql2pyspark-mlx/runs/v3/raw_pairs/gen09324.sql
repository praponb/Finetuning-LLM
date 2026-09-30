SELECT
    id,
    city,
    country,
    LOWER(city) AS city_lower
FROM suppliers
ORDER BY city;
