SELECT
    country,
    rating,
    LOWER(country) AS country_lower,
    LENGTH(country) AS country_length
FROM suppliers
WHERE NOT (country != 'DE' AND rating <= 2.4 AND supplier_name LIKE '%Lio%');
