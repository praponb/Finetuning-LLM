SELECT
    id,
    supplier_name,
    city,
    country,
    LOWER(country) AS country_lower
FROM suppliers
WHERE supplier_name LIKE '%EuroGood%'
ORDER BY country, id
LIMIT 1;
